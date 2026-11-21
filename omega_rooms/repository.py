from dataclasses import dataclass
from datetime import datetime
from threading import Lock
from typing import Iterable, Protocol
from uuid import UUID, uuid4

from .job import BookingRequest, DomainError, Error, Interval, Job, JobState, error


@dataclass(frozen=True)
class Room:
    id: str
    name: str
    capacity: int
    equipment: tuple[str, ...] = ()


class Repository(Protocol):
    def claim(
        self, owner: str, key: str, request: BookingRequest, now: datetime,
    ) -> tuple[Job, bool]: ...

    def start(self, job_id: UUID) -> Job: ...
    def get(self, job_id: UUID) -> Job: ...
    def reject(self, job_id: UUID, reason: Error) -> Job: ...
    def fail(self, job_id: UUID, reason: Error) -> Job: ...
    def release(self, job_id: UUID) -> None: ...
    def recover(self) -> tuple[Job, ...]: ...
    def get_for_owner(self, owner: str, job_id: UUID, now: datetime) -> Job: ...
    def cancel(self, owner: str, job_id: UUID, now: datetime) -> Job: ...
    def rooms(self) -> tuple[Room, ...]: ...
    def busy_rooms(self, room_ids: set[str], interval: Interval) -> set[str]: ...
    def confirm(self, job_id: UUID, interval: Interval) -> Job: ...


class InMemoryRepository:
    def __init__(self, rooms: Iterable[Room] = (), *, saved_jobs: Iterable[Job] = ()):
        self._lock = Lock()
        self._rooms = tuple(rooms)
        self._jobs = {job.id: job for job in saved_jobs}
        self._keys = {(job.owner_id, job.idempotency_key): job.id for job in self._jobs.values()}
        self._active: set[UUID] = set()

    def _get(self, job_id: UUID) -> Job:
        job = self._jobs.get(job_id)
        if job is None:
            raise DomainError(error("JOB_NOT_FOUND", "job_id", "Заявка не найдена"))
        return job

    def _get_for_owner(self, owner: str, job_id: UUID) -> Job:
        job = self._get(job_id)
        if job.owner_id != owner:
            raise DomainError(error("JOB_NOT_FOUND", "job_id", "Заявка не найдена"))
        return job

    def _save(self, job: Job) -> Job:
        self._jobs[job.id] = job
        return job

    def claim(
        self, owner: str, key: str, request: BookingRequest, now: datetime,
    ) -> tuple[Job, bool]:
        with self._lock:
            job_id = self._keys.get((owner, key))
            if job_id is not None:
                job = self._jobs[job_id]
                if job.request != request:
                    raise DomainError(error(
                        "IDEMPOTENCY_KEY_REUSED", "Idempotency-Key",
                        "Этот ключ уже использован с другими параметрами заявки",
                    ))
                return self._save(job.complete_if_due(now)), False

            job = self._save(Job(uuid4(), owner, key, request))
            self._keys[owner, key] = job.id
            self._active.add(job.id)
            return job, True

    def start(self, job_id: UUID) -> Job:
        with self._lock:
            return self._save(self._get(job_id).start())

    def get(self, job_id: UUID) -> Job:
        with self._lock:
            return self._get(job_id)

    def get_for_owner(self, owner: str, job_id: UUID, now: datetime) -> Job:
        with self._lock:
            return self._save(self._get_for_owner(owner, job_id).complete_if_due(now))

    def confirm(self, job_id: UUID, interval: Interval) -> Job:
        with self._lock:
            job = self._get(job_id)
            confirmed = job.confirm(interval)
            if self._busy_rooms({job.request.room_id}, interval):
                return self._save(job.reject(error(
                    "ROOM_UNAVAILABLE", "room_id", "Интервал пересекается с подтверждённой бронью",
                )))
            return self._save(confirmed)

    def reject(self, job_id: UUID, reason: Error) -> Job:
        with self._lock:
            return self._save(self._get(job_id).reject(reason))

    def fail(self, job_id: UUID, reason: Error) -> Job:
        with self._lock:
            return self._save(self._get(job_id).fail(reason))

    def cancel(self, owner: str, job_id: UUID, now: datetime) -> Job:
        with self._lock:
            job = self._save(self._get_for_owner(owner, job_id).complete_if_due(now))
            return self._save(job.cancel(now))

    def release(self, job_id: UUID) -> None:
        with self._lock:
            self._active.discard(job_id)

    def recover(self) -> tuple[Job, ...]:
        interruption = error("PROCESSING_INTERRUPTED", "$", "Проверка заявки прервана")
        with self._lock:
            recovered = []
            for job in self._jobs.values():
                if job.id in self._active:
                    continue
                if job.state == JobState.CREATED:
                    job = self._save(job.start())
                if job.state == JobState.CHECKING:
                    recovered.append(self._save(job.fail(interruption)))
            return tuple(recovered)

    def rooms(self) -> tuple[Room, ...]:
        return self._rooms

    def busy_rooms(self, room_ids: set[str], interval: Interval) -> set[str]:
        with self._lock:
            return self._busy_rooms(room_ids, interval)

    def _busy_rooms(self, room_ids: set[str], interval: Interval) -> set[str]:
        return {
            job.request.room_id
            for job in self._jobs.values()
            if job.state == JobState.CONFIRMED
            and job.request.room_id in room_ids
            and job.interval.overlaps(interval)
        }
