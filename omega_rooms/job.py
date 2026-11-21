from dataclasses import dataclass, replace
from datetime import datetime
from enum import Enum
from uuid import UUID


@dataclass(frozen=True)
class ErrorDetail:
    field: str
    issue: str


@dataclass(frozen=True)
class Error:
    code: str
    message: str
    details: tuple[ErrorDetail, ...]


def error(code: str, field: str, message: str) -> Error:
    return Error(code, message, (ErrorDetail(field, message),))


class DomainError(Exception):
    def __init__(self, reason: Error):
        super().__init__(reason.message)
        self.reason = reason

    @property
    def code(self) -> str:
        return self.reason.code

    @property
    def details(self) -> tuple[ErrorDetail, ...]:
        return self.reason.details


class JobState(str, Enum):
    CREATED = "created"
    CHECKING = "checking"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    FAILED = "failed"
    CANCELLED = "cancelled"
    COMPLETED = "completed"


@dataclass(frozen=True)
class BookingRequest:
    room_id: str
    start_at: str
    end_at: str


@dataclass(frozen=True)
class Interval:
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.start.utcoffset() is None or self.end.utcoffset() is None:
            raise ValueError("Границы интервала должны содержать смещение UTC")
        if self.end <= self.start:
            raise ValueError("Конец интервала должен быть позже его начала")

    def overlaps(self, other: "Interval") -> bool:
        return self.start < other.end and other.start < self.end


@dataclass(frozen=True)
class Job:
    id: UUID
    owner_id: str
    idempotency_key: str
    request: BookingRequest
    state: JobState = JobState.CREATED
    interval: Interval | None = None
    error: Error | None = None

    def _require_state(self, source: JobState, target: JobState) -> None:
        if self.state != source:
            raise DomainError(error(
                "INVALID_JOB_TRANSITION", "state",
                f"Переход из {self.state.value} в {target.value} запрещён",
            ))

    def start(self) -> "Job":
        self._require_state(JobState.CREATED, JobState.CHECKING)
        return replace(self, state=JobState.CHECKING)

    def confirm(self, interval: Interval) -> "Job":
        self._require_state(JobState.CHECKING, JobState.CONFIRMED)
        return replace(self, state=JobState.CONFIRMED, interval=interval)

    def reject(self, reason: Error) -> "Job":
        self._require_state(JobState.CHECKING, JobState.REJECTED)
        return replace(self, state=JobState.REJECTED, error=reason)

    def fail(self, reason: Error) -> "Job":
        self._require_state(JobState.CHECKING, JobState.FAILED)
        return replace(self, state=JobState.FAILED, error=reason)

    def cancel(self, now: datetime) -> "Job":
        self._require_state(JobState.CONFIRMED, JobState.CANCELLED)
        if now >= self.interval.start:
            raise DomainError(error(
                "CANCELLATION_TOO_LATE", "start_at", "Время бронирования уже началось",
            ))
        return replace(self, state=JobState.CANCELLED)

    def complete_if_due(self, now: datetime) -> "Job":
        if self.state == JobState.CONFIRMED and now >= self.interval.end:
            return replace(self, state=JobState.COMPLETED)
        return self
