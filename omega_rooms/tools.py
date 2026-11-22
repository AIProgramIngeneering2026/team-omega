import re
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from time import monotonic
from typing import Callable, Iterable, Mapping, Protocol

from .dispatcher import BookingWork, Rejection, utc_now
from .job import DomainError, Interval, Job, error
from .repository import Repository, Room


_TIMESTAMP = re.compile(
    r"(?P<seconds>[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt][0-9]{2}:[0-9]{2}:[0-9]{2})"
    r"(?:\.(?P<fraction>[0-9]+))?"
    r"(?P<offset>[Zz]|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])?"
)
_SCHEDULE_TIMEOUT = 5.0


class Schedule(Protocol):
    def busy_rooms(
        self, room_ids: set[str], interval: Interval, *, timeout: float,
    ) -> set[str]:
        """Вернуть занятые аудитории или прервать проверку в пределах общего срока."""
        ...


class InMemorySchedule:
    def __init__(self, slots: Mapping[str, Iterable[Interval]] | None = None):
        self._slots = {room_id: tuple(intervals) for room_id, intervals in (slots or {}).items()}

    def busy_rooms(
        self, room_ids: set[str], interval: Interval, *, timeout: float,
    ) -> set[str]:
        return {
            room_id
            for room_id in room_ids
            if any(interval.overlaps(busy) for busy in self._slots.get(room_id, ()))
        }


@dataclass(frozen=True)
class SearchRequest:
    start_at: str
    end_at: str
    participants: int
    equipment: tuple[str, ...]


@dataclass(frozen=True)
class RoomSearchResult:
    rooms: tuple[Room, ...]


def _parse_timestamp(value: str, field: str) -> datetime:
    invalid_format = error("INVALID_TIME_FORMAT", field, "Укажите полную дату и время")
    match = _TIMESTAMP.fullmatch(value) if isinstance(value, str) else None
    if match is None:
        raise DomainError(invalid_format)

    offset = match["offset"]
    fraction = (match["fraction"] or "").ljust(6, "0")[:6]
    timestamp = f"{match['seconds']}.{fraction}"
    try:
        if offset:
            timestamp += "+00:00" if offset.upper() == "Z" else offset
            return datetime.fromisoformat(timestamp).astimezone(timezone.utc)
        datetime.fromisoformat(timestamp)
    except (ValueError, OverflowError):
        raise DomainError(invalid_format) from None

    raise DomainError(error("TIMEZONE_REQUIRED", field, "Укажите смещение от UTC"))


def _validate_interval(start_at: str, end_at: str, now: datetime) -> Interval:
    start = _parse_timestamp(start_at, "start_at")
    end = _parse_timestamp(end_at, "end_at")
    if end <= start:
        raise DomainError(error("INVALID_INTERVAL", "end_at", "Конец должен быть позже начала"))
    if start < now:
        raise DomainError(error("START_IN_PAST", "start_at", "Начало интервала уже прошло"))
    return Interval(start, end)


def _check_schedule_deadline(started: float) -> None:
    if monotonic() - started >= _SCHEDULE_TIMEOUT:
        raise DomainError(error("SCHEDULE_TIMEOUT", "$", "Вышло время проверки расписания"))


def _scheduled_rooms(schedule: Schedule, room_ids: set[str], interval: Interval) -> set[str]:
    started = monotonic()
    try:
        busy = schedule.busy_rooms(room_ids, interval, timeout=_SCHEDULE_TIMEOUT)
    except Exception:
        _check_schedule_deadline(started)
        raise
    _check_schedule_deadline(started)
    if not isinstance(busy, set) or not busy <= room_ids:
        raise DomainError(error("SCHEDULE_INVALID_RESPONSE", "$", "Некорректный ответ расписания"))
    return busy


class SearchRoomsTool:
    name = "SearchRoomsTool"

    def __init__(
        self, repository: Repository, schedule: Schedule, clock: Callable[[], datetime] = utc_now,
    ):
        self._repository = repository
        self._schedule = schedule
        self._clock = clock

    def execute(self, params: SearchRequest) -> RoomSearchResult:
        try:
            interval = _validate_interval(params.start_at, params.end_at, self._clock())
        except DomainError as exc:
            raise DomainError(replace(exc.reason, code="VALIDATION_ERROR")) from None

        if type(params.participants) is not int or params.participants < 1:
            raise DomainError(error("VALIDATION_ERROR", "participants", "Укажите целое число от 1"))
        required_equipment: set[str] = set()
        if not isinstance(params.equipment, (tuple, list)) or len(params.equipment) > 100:
            raise DomainError(error("VALIDATION_ERROR", "equipment", "Укажите список до 100 кодов"))
        for index, code in enumerate(params.equipment):
            if not isinstance(code, str) or not 1 <= len(code) <= 128 or code in required_equipment:
                raise DomainError(error(
                    "VALIDATION_ERROR", f"equipment.{index}", "Укажите уникальный непустой код",
                ))
            required_equipment.add(code)

        candidates = {
            room.id: room
            for room in self._repository.rooms()
            if room.capacity >= params.participants and required_equipment.issubset(room.equipment)
        }
        if not candidates:
            return RoomSearchResult(())

        room_ids = set(candidates)
        busy = self._repository.busy_rooms(room_ids, interval)
        busy = busy | _scheduled_rooms(self._schedule, room_ids, interval)
        return RoomSearchResult(tuple(candidates[room_id] for room_id in sorted(room_ids - busy)))


class BookingTool:
    name = "BookingTool"

    def __init__(
        self, repository: Repository, schedule: Schedule, clock: Callable[[], datetime] = utc_now,
    ):
        self._repository = repository
        self._schedule = schedule
        self._clock = clock

    def execute(self, work: BookingWork) -> Job | Rejection:
        request = work.request
        try:
            interval = _validate_interval(request.start_at, request.end_at, self._clock())
        except DomainError as exc:
            return Rejection(exc.reason)

        if not any(room.id == request.room_id for room in self._repository.rooms()):
            return Rejection(error("ROOM_NOT_FOUND", "room_id", "Аудитория не найдена"))
        if _scheduled_rooms(self._schedule, {request.room_id}, interval):
            return Rejection(error(
                "ROOM_UNAVAILABLE", "room_id", "Интервал пересекается с учебным занятием",
            ))
        return self._repository.confirm(work.job_id, interval)
