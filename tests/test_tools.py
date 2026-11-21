from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Barrier
from unittest import TestCase

from omega_rooms.dispatcher import Dispatcher, Registry
from omega_rooms.job import BookingRequest, DomainError, Interval, JobState, error
from omega_rooms.repository import InMemoryRepository, Room
from omega_rooms.tools import BookingTool, InMemorySchedule, SearchRequest, SearchRoomsTool


NOW = datetime(2026, 11, 21, 8, tzinfo=timezone.utc)
START = "2026-11-21T15:00:00+05:00"
END = "2026-11-21T11:00:00Z"
ROOM_A = Room("A-301", "А-301", 30, ("projector",))
ROOM_B = Room("B-201", "Б-201", 40, ("projector",))
ROOM_C = Room("C-101", "В-101", 12)
ROOM_D = Room("D-101", "Г-101", 30, ("projector",))


class UnavailableSchedule:
    def busy_rooms(self, room_ids, interval, *, timeout):
        raise DomainError(error("SCHEDULE_TIMEOUT", "$", "Вышло время проверки"))


class ToolsTest(TestCase):
    def setup_tools(self, schedule):
        self.repo = InMemoryRepository([ROOM_D, ROOM_B, ROOM_A, ROOM_C])
        registry = Registry([
            BookingTool(self.repo, schedule, lambda: NOW),
            SearchRoomsTool(self.repo, schedule, lambda: NOW),
        ])
        self.dispatcher = Dispatcher(self.repo, registry, lambda: NOW)

    def setUp(self):
        self.setup_tools(InMemorySchedule())

    def book(self, room="A-301", *, start=START, end=END, key="key"):
        return self.dispatcher.book("BookingTool", "alice", key, BookingRequest(room, start, end))

    def search(self, start=START, end=END, participants=25, equipment=("projector",)):
        request = SearchRequest(start, end, participants, equipment)
        return self.dispatcher.execute("SearchRoomsTool", request)

    def test_search_filters_equipment_and_capacity_and_sorts_rooms(self):
        self.assertEqual(self.search().rooms, (ROOM_A, ROOM_B, ROOM_D))
        self.assertEqual(self.search(equipment=("unknown",)).rooms, ())

    def test_search_excludes_both_schedule_and_local_bookings(self):
        self.setup_tools(InMemorySchedule({
            "B-201": [Interval(NOW.replace(hour=9), NOW.replace(hour=10, minute=30))],
        }))
        self.assertEqual(self.book().state, JobState.CONFIRMED)
        self.assertEqual(self.search().rooms, (ROOM_D,))
        self.assertEqual(self.search(start=END, end="2026-11-21T12:00:00Z").rooms,
                         (ROOM_A, ROOM_B, ROOM_D))

    def test_search_without_candidates_does_not_require_schedule(self):
        self.setup_tools(UnavailableSchedule())
        self.assertEqual(self.search(participants=100).rooms, ())

    def test_search_propagates_schedule_timeout_instead_of_empty_results(self):
        self.setup_tools(UnavailableSchedule())
        with self.assertRaises(DomainError) as caught:
            self.search()
        self.assertEqual(caught.exception.code, "SCHEDULE_TIMEOUT")

    def test_search_validation_error_names_field(self):
        self.setup_tools(UnavailableSchedule())
        for args, field in (
            ({"start": "2026-11-21T10:00:00"}, "start_at"),
            ({"end": START}, "end_at"),
            ({"participants": 0}, "participants"),
            ({"equipment": ("projector", "")}, "equipment.1"),
        ):
            with self.subTest(args=args), self.assertRaises(DomainError) as caught:
                self.search(**args)
            self.assertEqual(caught.exception.code, "VALIDATION_ERROR")
            self.assertEqual(caught.exception.details[0].field, field)

    def test_booking_preserves_validation_reasons_and_checks_before_schedule(self):
        self.setup_tools(UnavailableSchedule())
        for index, (args, code, field) in enumerate((
            ({"start": ""}, "INVALID_TIME_FORMAT", "start_at"),
            ({"start": "2026-11-21T10:00:00"}, "TIMEZONE_REQUIRED", "start_at"),
            ({"start": "2026-11-21T10:00:00+00:60"}, "INVALID_TIME_FORMAT", "start_at"),
            ({"end": START}, "INVALID_INTERVAL", "end_at"),
            ({"start": "2026-11-21T07:00:00Z"}, "START_IN_PAST", "start_at"),
            ({"room": "missing"}, "ROOM_NOT_FOUND", "room_id"),
        )):
            with self.subTest(code=code):
                job = self.book(key=str(index), **args)
                self.assertEqual(job.state, JobState.REJECTED)
                self.assertEqual(job.error.code, code)
                self.assertEqual(job.error.details[0].field, field)

    def test_booking_rejects_conflict_with_schedule(self):
        self.setup_tools(InMemorySchedule({
            "A-301": [Interval(NOW.replace(hour=9), NOW.replace(hour=11))],
        }))
        job = self.book()
        self.assertEqual(job.state, JobState.REJECTED)
        self.assertEqual(job.error.code, "ROOM_UNAVAILABLE")
        self.assertEqual(job.error.details[0].field, "room_id")

    def test_booking_timeout_is_saved_as_failed(self):
        self.setup_tools(UnavailableSchedule())
        job = self.book()
        self.assertEqual(job.state, JobState.FAILED)
        self.assertEqual(job.error.code, "SCHEDULE_TIMEOUT")
        self.assertEqual(job.error.details[0].field, "$")

    def test_live_recovery_after_storage_refuses_confirmation(self):
        class TemporarilyUnavailableRepository(InMemoryRepository):
            available = False

            def confirm(self, job_id, interval):
                if not self.available:
                    raise DomainError(error("STORAGE_UNAVAILABLE", "$", "Хранилище недоступно"))
                return super().confirm(job_id, interval)

        repo = TemporarilyUnavailableRepository([ROOM_A])
        registry = Registry([BookingTool(repo, InMemorySchedule(), lambda: NOW)])
        self.dispatcher = Dispatcher(repo, registry, lambda: NOW)
        with self.assertRaises(DomainError) as caught:
            self.book()
        self.assertEqual(caught.exception.code, "STORAGE_UNAVAILABLE")

        repo.available = True
        recovered, = self.dispatcher.recover()
        self.assertEqual(recovered.state, JobState.FAILED)
        self.assertEqual(recovered.error.code, "PROCESSING_INTERRUPTED")
        self.assertEqual(self.book(), recovered)

    def test_booking_rejects_malformed_schedule_as_a_technical_failure(self):
        class MalformedSchedule(InMemorySchedule):
            def busy_rooms(self, room_ids, interval, *, timeout):
                return {"unknown-room"}

        self.setup_tools(MalformedSchedule())
        job = self.book()
        self.assertEqual(job.state, JobState.FAILED)
        self.assertEqual(job.error.code, "SCHEDULE_INVALID_RESPONSE")
        self.assertEqual(self.repo.busy_rooms({"A-301"}, Interval(
            NOW.replace(hour=10), NOW.replace(hour=11),
        )), set())

    def test_concurrent_bookings_cannot_both_confirm_the_same_room(self):
        barrier = Barrier(2, timeout=3)

        class ConcurrentSchedule(InMemorySchedule):
            def busy_rooms(self, room_ids, interval, *, timeout):
                barrier.wait()
                return super().busy_rooms(room_ids, interval, timeout=timeout)

        self.setup_tools(ConcurrentSchedule())
        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(self.book, key="first")
            second = executor.submit(self.book, key="second")
            jobs = [first.result(timeout=5), second.result(timeout=5)]
        self.assertCountEqual([job.state for job in jobs], [JobState.CONFIRMED, JobState.REJECTED])
        rejected = next(job for job in jobs if job.state == JobState.REJECTED)
        self.assertEqual(rejected.error.code, "ROOM_UNAVAILABLE")

    def test_fractional_start_and_explicit_offset_are_compared_in_utc(self):
        job = self.book(start="2026-11-21T15:00:00.1+05:00")
        self.assertEqual(job.state, JobState.CONFIRMED)
        self.assertEqual(job.interval.start, NOW.replace(hour=10, microsecond=100000))
