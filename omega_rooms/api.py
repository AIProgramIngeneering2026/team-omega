"""HTTP API for Omega Rooms. Domain decisions remain in Dispatcher and Job."""

import json
import re
from datetime import datetime, timezone
from typing import Mapping
from uuid import UUID

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from .dispatcher import Dispatcher
from .job import BookingRequest, DomainError, Error, error
from .tools import RoomSearchResult, SearchRequest


_IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


def _error_body(reason: Error) -> dict[str, object]:
    return {
        "code": reason.code,
        "message": reason.message,
        "details": [{"field": item.field, "issue": item.issue} for item in reason.details],
    }


def _status_for(code: str) -> int:
    if code == "UNAUTHORIZED":
        return 401
    if code == "BAD_JSON":
        return 400
    if code == "PAYLOAD_TOO_LARGE":
        return 413
    if code == "UNSUPPORTED_MEDIA_TYPE":
        return 415
    if code == "METHOD_NOT_ALLOWED":
        return 405
    if code in {"JOB_NOT_FOUND", "ROUTE_NOT_FOUND"}:
        return 404
    if code in {"INVALID_JOB_TRANSITION", "CANCELLATION_TOO_LATE", "IDEMPOTENCY_KEY_REUSED"}:
        return 409
    if code in {"VALIDATION_ERROR", "INVALID_TIME_FORMAT", "TIMEZONE_REQUIRED", "INVALID_INTERVAL", "START_IN_PAST"}:
        return 422
    if code == "STORAGE_UNAVAILABLE":
        return 503
    if code == "SCHEDULE_TIMEOUT":
        return 504
    if code in {"SCHEDULE_UNAVAILABLE", "SCHEDULE_INVALID_RESPONSE"}:
        return 502
    return 500


def _timestamp(job: object, name: str, fallback: datetime) -> str:
    value = getattr(job, name, None) or fallback
    if isinstance(value, str):
        return value[:-6] + "Z" if value.endswith("+00:00") else value
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _job_body(job: object) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    request = job.request
    body: dict[str, object] = {
        "id": str(job.id),
        "room_id": request.room_id,
        "start_at": request.start_at,
        "end_at": request.end_at,
        "state": job.state.value,
        "created_at": _timestamp(job, "created_at", now),
        "updated_at": _timestamp(job, "updated_at", now),
    }
    if job.error is not None:
        body["error"] = _error_body(job.error)
    return body


def _bad_request(field: str, issue: str) -> DomainError:
    return DomainError(error("VALIDATION_ERROR", field, issue))


async def _json_object(request: Request) -> dict[str, object]:
    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if content_type != "application/json":
        raise DomainError(error("UNSUPPORTED_MEDIA_TYPE", "Content-Type", "Требуется application/json"))
    try:
        raw = await request.body()
        if len(raw) > 65536:
            raise DomainError(error("PAYLOAD_TOO_LARGE", "$", "Размер тела запроса превышает 64 КиБ"))
        value = json.loads(raw.decode("utf-8"))
    except DomainError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise DomainError(error("BAD_JSON", "$", "Тело запроса должно быть корректным JSON")) from None
    if not isinstance(value, dict):
        raise _bad_request("$", "Тело запроса должно быть JSON-объектом")
    return value


def _fields(body: dict[str, object], required: set[str], allowed: set[str]) -> None:
    extra = sorted(set(body) - allowed)
    if extra:
        raise _bad_request(extra[0], "Неизвестное поле")
    missing = sorted(required - set(body))
    if missing:
        raise _bad_request(missing[0], "Обязательное поле отсутствует")


def _string(body: dict[str, object], field: str, maximum: int, *, nonempty: bool = True) -> str:
    value = body[field]
    if not isinstance(value, str) or len(value) > maximum or (nonempty and not value):
        raise _bad_request(field, f"Требуется строка длиной до {maximum} символов")
    return value


def create_app(dispatcher: Dispatcher, token_owners: Mapping[str, str]) -> FastAPI:
    """Build an API app around an existing dispatcher and pre-issued bearer tokens."""
    app = FastAPI(title="Omega Rooms API", docs_url=None, redoc_url=None, openapi_url=None)

    @app.middleware("http")
    async def no_store(request: Request, call_next):
        content_length = request.headers.get("content-length")
        if content_length and content_length.isdecimal() and int(content_length) > 65536:
            reason = error("PAYLOAD_TOO_LARGE", "$", "Размер тела запроса превышает 64 КиБ")
            return JSONResponse(_error_body(reason), status_code=413, headers={"Cache-Control": "no-store"})
        chunks: list[bytes] = []
        size = 0
        async for chunk in request.stream():
            size += len(chunk)
            if size > 65536:
                reason = error("PAYLOAD_TOO_LARGE", "$", "Размер тела запроса превышает 64 КиБ")
                return JSONResponse(_error_body(reason), status_code=413, headers={"Cache-Control": "no-store"})
            chunks.append(chunk)
        body = b"".join(chunks)
        request._body = body

        async def replay_body():
            return {"type": "http.request", "body": body, "more_body": False}

        request._receive = replay_body
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.exception_handler(DomainError)
    async def domain_error_handler(request: Request, exc: DomainError):
        headers = {"WWW-Authenticate": "Bearer"} if exc.code == "UNAUTHORIZED" else None
        return JSONResponse(_error_body(exc.reason), status_code=_status_for(exc.code), headers=headers)

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(request: Request, exc: StarletteHTTPException):
        if exc.status_code == 405:
            reason = error("METHOD_NOT_ALLOWED", "$", "Метод не поддерживается для этого маршрута")
            headers = dict(exc.headers or {})
            return JSONResponse(_error_body(reason), status_code=405, headers=headers)
        reason = error("ROUTE_NOT_FOUND", "$", "Маршрут не найден")
        return JSONResponse(_error_body(reason), status_code=404)

    @app.exception_handler(Exception)
    async def internal_error_handler(request: Request, exc: Exception):
        reason = error("INTERNAL_ERROR", "$", "Не удалось выполнить запрос")
        return JSONResponse(_error_body(reason), status_code=500)

    def owner(request: Request) -> str:
        authorization = request.headers.get("authorization", "")
        scheme, _, token = authorization.partition(" ")
        result = token_owners.get(token) if scheme.lower() == "bearer" and token else None
        if result is None:
            raise DomainError(error("UNAUTHORIZED", "Authorization", "Передайте действительный Bearer-токен"))
        return result

    @app.post("/rooms/search")
    async def search_rooms(request: Request):
        owner(request)
        body = await _json_object(request)
        fields = {"start_at", "end_at", "participants", "equipment"}
        _fields(body, fields, fields)
        start = _string(body, "start_at", 64, nonempty=False)
        end = _string(body, "end_at", 64, nonempty=False)
        participants = body["participants"]
        if type(participants) is not int or participants < 1:
            raise _bad_request("participants", "Требуется целое число не меньше 1")
        equipment = body["equipment"]
        if not isinstance(equipment, list) or len(equipment) > 100:
            raise _bad_request("equipment", "Требуется список не более чем из 100 кодов")
        if any(not isinstance(value, str) or not 1 <= len(value) <= 128 for value in equipment):
            raise _bad_request("equipment", "Код оборудования должен быть строкой длиной 1–128 символов")
        if len(set(equipment)) != len(equipment):
            raise _bad_request("equipment", "Коды оборудования не должны повторяться")
        result: RoomSearchResult = dispatcher.execute(
            "SearchRoomsTool", SearchRequest(start, end, participants, tuple(equipment)),
        )
        return {"rooms": [
            {"id": room.id, "name": room.name, "capacity": room.capacity, "equipment": list(room.equipment)}
            for room in result.rooms
        ]}

    @app.post("/jobs")
    async def create_job(request: Request):
        user = owner(request)
        key = request.headers.get("Idempotency-Key", "")
        if not _IDEMPOTENCY_KEY.fullmatch(key):
            raise _bad_request("Idempotency-Key", "Передайте ключ длиной 1–128 символов из разрешённых знаков")
        body = await _json_object(request)
        fields = {"room_id", "start_at", "end_at"}
        _fields(body, fields, fields)
        booking = BookingRequest(
            _string(body, "room_id", 128),
            _string(body, "start_at", 64, nonempty=False),
            _string(body, "end_at", 64, nonempty=False),
        )
        job, created = dispatcher.book_with_status("BookingTool", user, key, booking)
        status = 201 if created else 200
        return JSONResponse(
            _job_body(job), status_code=status,
            headers={"Location": f"/jobs/{job.id}"},
        )

    @app.get("/jobs/{job_id}")
    async def get_job(job_id: str, request: Request):
        user = owner(request)
        try:
            parsed_id = UUID(job_id)
        except ValueError:
            raise _bad_request("job_id", "Ожидается UUID") from None
        return _job_body(dispatcher.get(user, parsed_id))

    @app.post("/jobs/{job_id}/cancel")
    async def cancel_job(job_id: str, request: Request):
        user = owner(request)
        try:
            parsed_id = UUID(job_id)
        except ValueError:
            raise _bad_request("job_id", "Ожидается UUID") from None
        return _job_body(dispatcher.cancel(user, parsed_id))

    return app
