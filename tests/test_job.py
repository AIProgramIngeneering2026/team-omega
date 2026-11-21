from datetime import datetime, timezone
from threading import Event, Thread
from unittest import TestCase

from omega_rooms.dispatcher import Dispatcher, Registry
from omega_rooms.job import BookingRequest, DomainError, Interval, JobState, error
from omega_rooms.repository import InMemoryRepository


NOW = datetime(2026, 11, 21, 8, tzinfo=timezone.utc)
SLOT = Interval(NOW.replace(hour=10), NOW.replace(hour=11))
REQUEST = BookingRequest("A-301", "2026-11-21T10:00:00Z", "2026-11-21T11:00:00Z")


class BookingDouble:
    name = "BookingTool"

    def __init__(self, repository):
        self.repository = repository

    def execute(self, work):
        return self.repository.confirm(work.job_id, SLOT)


class JobTest(TestCase):
    def setUp(self):
        self.repo = InMemoryRepository()
        self.dispatcher = Dispatcher(self.repo, Registry([BookingDouble(self.repo)]), lambda: NOW)

    def new_job(self, key="key"):
        job, _ = self.repo.claim("alice", key, REQUEST, lambda: NOW)
        self.repo.release(job.id)
        return job

    def book(self, key="key"):
        return self.dispatcher.book("BookingTool", "alice", key, REQUEST)

    def assert_transition_rejected(self, operation):
        with self.assertRaises(DomainError) as caught:
            operation()
        self.assertEqual(caught.exception.code, "INVALID_JOB_TRANSITION")
        self.assertEqual(caught.exception.details[0].field, "state")

    def test_booking_runs_from_created_to_confirmed(self):
        job = self.book()
        self.assertEqual(job.state, JobState.CONFIRMED)
        self.assertEqual(job.interval, SLOT)
        self.assertIsNone(job.error)

    def test_z1_duplicate_requests_start_only_one_tool(self):
        entered = Event()
        release = Event()
        results = []

        class SlowBooking(BookingDouble):
            def execute(self, work):
                entered.set()
                if not release.wait(3):
                    raise RuntimeError("test did not release the tool")
                return super().execute(work)

        self.dispatcher = Dispatcher(self.repo, Registry([SlowBooking(self.repo)]), lambda: NOW)
        thread = Thread(target=lambda: results.append(self.book()))
        thread.start()
        try:
            self.assertTrue(entered.wait(2))
            duplicate = self.book()
            self.assertEqual(duplicate.state, JobState.CHECKING)
            self.assert_transition_rejected(lambda: self.repo.start(duplicate.id))
            self.assertEqual(self.dispatcher.recover(), ())
        finally:
            release.set()
            thread.join(4)
        self.assertFalse(thread.is_alive())
        self.assertEqual(results[0].id, duplicate.id)
        self.assertEqual(results[0].state, JobState.CONFIRMED)

    def test_z2_recover_created_and_checking_after_restart(self):
        created = self.new_job()
        checking = self.repo.start(self.new_job("interrupted").id)
        restarted = InMemoryRepository(saved_jobs=(created, checking))
        dispatcher = Dispatcher(restarted, Registry([]), lambda: NOW)
        recovered = dispatcher.recover()
        self.assertEqual({job.id for job in recovered}, {created.id, checking.id})
        for job in recovered:
            self.assertEqual(job.state, JobState.FAILED)
            self.assertEqual(job.error.code, "PROCESSING_INTERRUPTED")
        self.assertEqual(dispatcher.recover(), ())

    def test_z3_cancelled_job_cannot_be_confirmed_by_late_result(self):
        job = self.book()
        self.dispatcher.cancel("alice", job.id)
        self.assert_transition_rejected(lambda: self.repo.confirm(job.id, SLOT))
        self.assertEqual(self.repo.get(job.id).state, JobState.CANCELLED)
        self.assertEqual(self.book("replacement").state, JobState.CONFIRMED)

    def test_z4_failed_job_cannot_be_confirmed_by_late_result(self):
        job = self.repo.start(self.new_job().id)
        self.repo.fail(job.id, error("SCHEDULE_TIMEOUT", "$", "Вышло время проверки"))
        self.assert_transition_rejected(lambda: self.repo.confirm(job.id, SLOT))
        self.assertEqual(self.repo.get(job.id).error.code, "SCHEDULE_TIMEOUT")
        self.assertEqual(self.book("replacement").state, JobState.CONFIRMED)

    def test_z5_created_job_cannot_be_confirmed(self):
        job = self.new_job()
        self.assert_transition_rejected(lambda: self.repo.confirm(job.id, SLOT))
        self.assertEqual(self.repo.get(job.id).state, JobState.CREATED)

    def test_z6_confirmed_job_cannot_start_again(self):
        job = self.book()
        self.assert_transition_rejected(lambda: self.repo.start(job.id))
        self.assertEqual(self.book(), job)

    def test_unknown_operation_is_a_domain_error(self):
        with self.assertRaises(DomainError) as caught:
            self.dispatcher.book("missing", "alice", "key", REQUEST)
        self.assertEqual(caught.exception.code, "UNKNOWN_OPERATION")
        self.assertEqual(caught.exception.details[0].field, "operation")

    def test_tool_crash_fails_the_job(self):
        class BrokenTool:
            name = "other"

            def execute(self, work):
                raise RuntimeError("private implementation detail")

        dispatcher = Dispatcher(self.repo, Registry([BrokenTool()]), lambda: NOW)
        job = dispatcher.book("other", "alice", "key", REQUEST)
        self.assertEqual(job.state, JobState.FAILED)
        self.assertEqual(job.error.code, "INTERNAL_ERROR")
        self.assertNotIn("private implementation detail", job.error.message)

    def test_cancel_between_tool_commit_and_return_preserves_current_state(self):
        repo = self.repo

        class BookingThenCancellation(BookingDouble):
            def execute(self, work):
                result = super().execute(work)
                repo.cancel("alice", work.job_id, lambda: NOW)
                return result

        dispatcher = Dispatcher(repo, Registry([BookingThenCancellation(repo)]), lambda: NOW)
        job = dispatcher.book("BookingTool", "alice", "key", REQUEST)
        self.assertEqual(job.state, JobState.CANCELLED)

    def test_booking_reads_completed_state_if_interval_ends_before_tool_returns(self):
        current_time = NOW

        class BookingAtEnd(BookingDouble):
            def execute(self, work):
                nonlocal current_time
                confirmed = super().execute(work)
                current_time = SLOT.end
                return confirmed

        dispatcher = Dispatcher(
            self.repo, Registry([BookingAtEnd(self.repo)]), lambda: current_time,
        )
        job = dispatcher.book("BookingTool", "alice", "key", REQUEST)
        self.assertEqual(job.state, JobState.COMPLETED)

    def test_cancel_checks_time_after_waiting_for_repository(self):
        current_time = NOW

        class QueuedCancellation(InMemoryRepository):
            def cancel(self, owner, job_id, clock):
                nonlocal current_time
                current_time = SLOT.start
                return super().cancel(owner, job_id, clock)

        repo = QueuedCancellation()
        dispatcher = Dispatcher(repo, Registry([BookingDouble(repo)]), lambda: current_time)
        job = dispatcher.book("BookingTool", "alice", "key", REQUEST)
        with self.assertRaises(DomainError) as caught:
            dispatcher.cancel("alice", job.id)
        self.assertEqual(caught.exception.code, "CANCELLATION_TOO_LATE")

    def test_different_body_cannot_reuse_key(self):
        self.book()
        with self.assertRaises(DomainError) as caught:
            self.dispatcher.book("BookingTool", "alice", "key", BookingRequest("B-201", "", ""))
        self.assertEqual(caught.exception.code, "IDEMPOTENCY_KEY_REUSED")

    def test_wrong_owner_cannot_read_or_cancel_job(self):
        job = self.book()
        for command in (self.dispatcher.get, self.dispatcher.cancel):
            with self.assertRaises(DomainError) as caught:
                command("bob", job.id)
            self.assertEqual(caught.exception.code, "JOB_NOT_FOUND")
        self.assertEqual(self.repo.get(job.id).state, JobState.CONFIRMED)

    def test_cancel_at_start_is_denied(self):
        job = self.book()
        dispatcher = Dispatcher(self.repo, Registry([]), lambda: SLOT.start)
        with self.assertRaises(DomainError) as caught:
            dispatcher.cancel("alice", job.id)
        self.assertEqual(caught.exception.code, "CANCELLATION_TOO_LATE")

    def test_read_at_end_completes_booking(self):
        job = self.book()
        dispatcher = Dispatcher(self.repo, Registry([]), lambda: SLOT.end)
        self.assertEqual(dispatcher.get("alice", job.id).state, JobState.COMPLETED)
