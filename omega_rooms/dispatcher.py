from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Protocol, TypeVar
from uuid import UUID

from .job import BookingRequest, DomainError, Error, Job, JobState, error
from .repository import Repository


Params = TypeVar("Params", contravariant=True)
Result = TypeVar("Result", covariant=True)


class Tool(Protocol[Params, Result]):
    @property
    def name(self) -> str: ...

    def execute(self, params: Params) -> Result: ...


class Registry:
    def __init__(self, tools: Iterable[Tool[Any, Any]]):
        self._tools: dict[str, Tool[Any, Any]] = {}
        for tool in tools:
            if tool.name in self._tools:
                raise ValueError(f"Операция {tool.name} уже зарегистрирована")
            self._tools[tool.name] = tool

    def get(self, operation: str) -> Tool[Any, Any]:
        try:
            return self._tools[operation]
        except KeyError:
            raise DomainError(error("UNKNOWN_OPERATION", "operation", "Неизвестная операция")) from None


@dataclass(frozen=True)
class BookingWork:
    job_id: UUID
    request: BookingRequest


@dataclass(frozen=True)
class Rejection:
    reason: Error


@dataclass(frozen=True)
class Failure:
    reason: Error


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Dispatcher:
    def __init__(
        self, repository: Repository, registry: Registry, clock: Callable[[], datetime] = utc_now,
    ):
        self._repository = repository
        self._registry = registry
        self._clock = clock

    def execute(self, operation: str, params: object) -> Any:
        tool = self._registry.get(operation)
        try:
            return tool.execute(params)
        except DomainError:
            raise
        except Exception:
            raise DomainError(self._internal_error()) from None

    def book(self, operation: str, owner: str, key: str, request: BookingRequest) -> Job:
        tool = self._registry.get(operation)
        job, claimed = self._repository.claim(owner, key, request, self._clock())
        if not claimed:
            return job
        try:
            self._repository.start(job.id)
            try:
                outcome = tool.execute(BookingWork(job.id, job.request))
            except DomainError as exc:
                if exc.code == "STORAGE_UNAVAILABLE":
                    raise
                outcome = Failure(exc.reason)
            except Exception:
                outcome = Failure(self._internal_error())

            saved = self._repository.get(job.id)
            if saved.state != JobState.CHECKING:
                # Tool мог сохранить исход, а владелец — успеть отменить бронь.
                return saved
            if isinstance(outcome, Rejection):
                return self._repository.reject(job.id, outcome.reason)
            if isinstance(outcome, Failure):
                return self._repository.fail(job.id, outcome.reason)
            return self._repository.fail(job.id, self._internal_error())
        finally:
            self._repository.release(job.id)

    def get(self, owner: str, job_id: UUID) -> Job:
        return self._repository.get_for_owner(owner, job_id, self._clock())

    def cancel(self, owner: str, job_id: UUID) -> Job:
        return self._repository.cancel(owner, job_id, self._clock())

    def recover(self) -> tuple[Job, ...]:
        return self._repository.recover()

    @staticmethod
    def _internal_error() -> Error:
        return error("INTERNAL_ERROR", "$", "Не удалось завершить операцию")
