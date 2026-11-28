import json
import logging
import sqlite3
from contextlib import closing, contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Callable, Iterable, Iterator
from uuid import UUID, uuid4

from .job import BookingRequest, DomainError, Error, ErrorDetail, Interval, Job, JobState, error
from .repository import Room


logger = logging.getLogger(__name__)


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _read_job(row: sqlite3.Row) -> Job:
    interval = None
    if row["interval_start"] is not None:
        interval = Interval(
            datetime.fromisoformat(row["interval_start"]),
            datetime.fromisoformat(row["interval_end"]),
        )

    outcome_error = None
    if row["outcome_error"] is not None:
        data = json.loads(row["outcome_error"])
        outcome_error = Error(
            data["code"], data["message"],
            tuple(ErrorDetail(**detail) for detail in data["details"]),
        )

    return Job(
        UUID(row["id"]), row["owner_id"], row["idempotency_key"],
        BookingRequest(row["room_id"], row["request_start_at"], row["request_end_at"]),
        JobState(row["state"]), interval, outcome_error,
    )


class SQLiteRepository:
    """Use one shared instance per service process for active-job recovery."""

    def __init__(self, database: str | Path):
        self._database_uri = Path(database).resolve().as_uri() + "?mode=rw"
        self._active: set[UUID] = set()
        self._active_lock = Lock()

    @contextmanager
    def _connection(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        try:
            with closing(sqlite3.connect(self._database_uri, uri=True)) as connection, connection:
                connection.row_factory = sqlite3.Row
                connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
                yield connection
        except sqlite3.Error as exc:
            logger.exception("Ошибка хранилища SQLite")
            raise DomainError(error("STORAGE_UNAVAILABLE", "$", "Хранилище недоступно")) from exc

    @staticmethod
    def _get(connection: sqlite3.Connection, job_id: UUID, owner: str | None = None) -> Job:
        row = connection.execute("SELECT * FROM jobs WHERE id = ?", (str(job_id),)).fetchone()
        if row is None or (owner is not None and row["owner_id"] != owner):
            raise DomainError(error("JOB_NOT_FOUND", "job_id", "Заявка не найдена"))
        return _read_job(row)

    @staticmethod
    def _save(connection: sqlite3.Connection, job: Job) -> Job:
        interval = job.interval
        connection.execute(
            """
            UPDATE jobs
            SET state = ?, interval_start = ?, interval_end = ?, outcome_error = ?
            WHERE id = ?
            """,
            (
                job.state.value,
                _timestamp(interval.start) if interval else None,
                _timestamp(interval.end) if interval else None,
                json.dumps(asdict(job.error), ensure_ascii=False) if job.error else None,
                str(job.id),
            ),
        )
        return job

    def _complete_if_due(self, connection: sqlite3.Connection, job: Job, now: datetime) -> Job:
        completed = job.complete_if_due(now)
        if completed is not job:
            self._save(connection, completed)
        return completed

    def claim(
        self, owner: str, key: str, request: BookingRequest, clock: Callable[[], datetime],
    ) -> tuple[Job, bool]:
        with self._active_lock:
            with self._connection(write=True) as connection:
                row = connection.execute(
                    "SELECT * FROM jobs WHERE owner_id = ? AND idempotency_key = ?", (owner, key),
                ).fetchone()
                if row is not None:
                    job = _read_job(row)
                    if job.request != request:
                        raise DomainError(error(
                            "IDEMPOTENCY_KEY_REUSED", "Idempotency-Key",
                            "Этот ключ уже использован с другими параметрами заявки",
                        ))
                    return self._complete_if_due(connection, job, clock()), False

                job = Job(uuid4(), owner, key, request)
                connection.execute(
                    """
                    INSERT INTO jobs (
                        id, owner_id, idempotency_key, room_id, request_start_at, request_end_at, state
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (str(job.id), owner, key, request.room_id, request.start_at,
                     request.end_at, job.state.value),
                )
            self._active.add(job.id)
            return job, True

    def start(self, job_id: UUID) -> Job:
        with self._connection(write=True) as connection:
            return self._save(connection, self._get(connection, job_id).start())

    def get_for_owner(self, owner: str, job_id: UUID, clock: Callable[[], datetime]) -> Job:
        with self._connection(write=True) as connection:
            return self._complete_if_due(connection, self._get(connection, job_id, owner), clock())

    def confirm(self, job_id: UUID, interval: Interval) -> Job:
        with self._connection(write=True) as connection:
            job = self._get(connection, job_id)
            try:
                confirmed = job.confirm(interval)
            except DomainError:
                logger.info("Отклонено подтверждение Job %s из состояния %s", job.id, job.state.value)
                raise
            if self._busy_rooms(connection, {job.request.room_id}, interval):
                confirmed = job.reject(error(
                    "ROOM_UNAVAILABLE", "room_id", "Интервал пересекается с подтверждённой бронью",
                ))
            return self._save(connection, confirmed)

    def reject(self, job_id: UUID, reason: Error) -> Job:
        with self._connection(write=True) as connection:
            return self._save(connection, self._get(connection, job_id).reject(reason))

    def fail(self, job_id: UUID, reason: Error) -> Job:
        with self._connection(write=True) as connection:
            return self._save(connection, self._get(connection, job_id).fail(reason))

    def cancel(self, owner: str, job_id: UUID, clock: Callable[[], datetime]) -> Job:
        with self._connection(write=True) as connection:
            job = self._get(connection, job_id, owner)
            now = clock()
            completed = self._complete_if_due(connection, job, now)
            if completed is job:
                return self._save(connection, job.cancel(now))

        # Preserve lazy completion even when cancellation is rejected.
        return completed.cancel(now)

    def release(self, job_id: UUID) -> None:
        with self._active_lock:
            self._active.discard(job_id)

    def recover(self) -> tuple[Job, ...]:
        interruption = error("PROCESSING_INTERRUPTED", "$", "Проверка заявки прервана")
        with self._active_lock, self._connection(write=True) as connection:
            rows = connection.execute(
                "SELECT * FROM jobs WHERE state IN (?, ?)",
                (JobState.CREATED.value, JobState.CHECKING.value),
            ).fetchall()
            recovered = []
            for row in rows:
                job = _read_job(row)
                if job.id in self._active:
                    continue
                if job.state == JobState.CREATED:
                    job = job.start()
                recovered.append(self._save(connection, job.fail(interruption)))
            return tuple(recovered)

    def upsert_rooms(self, rooms: Iterable[Room]) -> None:
        entries = [(room.id, room.name, room.capacity, json.dumps(room.equipment)) for room in rooms]
        with self._connection(write=True) as connection:
            connection.executemany(
                """
                INSERT INTO rooms (id, name, capacity, equipment) VALUES (?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name, capacity = excluded.capacity, equipment = excluded.equipment
                """,
                entries,
            )

    def rooms(self) -> tuple[Room, ...]:
        with self._connection() as connection:
            return tuple(
                Room(row["id"], row["name"], row["capacity"], tuple(json.loads(row["equipment"])))
                for row in connection.execute("SELECT id, name, capacity, equipment FROM rooms ORDER BY id")
            )

    def busy_rooms(self, room_ids: set[str], interval: Interval) -> set[str]:
        with self._connection() as connection:
            return self._busy_rooms(connection, room_ids, interval)

    @staticmethod
    def _busy_rooms(
        connection: sqlite3.Connection, room_ids: set[str], interval: Interval,
    ) -> set[str]:
        start, end = _timestamp(interval.start), _timestamp(interval.end)
        busy = set()
        ids = tuple(room_ids)
        # Keep each query below SQLite's parameter limit, even for a large catalog.
        for offset in range(0, len(ids), 500):
            batch = ids[offset:offset + 500]
            placeholders = ", ".join("?" for _ in batch)
            rows = connection.execute(
                f"""
                SELECT DISTINCT room_id FROM jobs
                WHERE state = 'confirmed' AND interval_end > ? AND interval_start < ?
                  AND room_id IN ({placeholders})
                """,
                (start, end, *batch),
            )
            busy.update(row["room_id"] for row in rows)
        return busy
