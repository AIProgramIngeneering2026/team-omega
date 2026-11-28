import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

from fastapi.testclient import TestClient

from omega_rooms.api import create_app
from omega_rooms.job import BookingRequest, JobState
from omega_rooms.repository import Room
from omega_rooms.tools import RoomSearchResult


@dataclass
class FakeJob:
    id: object
    request: BookingRequest
    state: JobState = JobState.CONFIRMED
    error: object = None
    created_at: datetime = datetime(2026, 1, 1, tzinfo=timezone.utc)
    updated_at: datetime = datetime(2026, 1, 1, tzinfo=timezone.utc)


class FakeDispatcher:
    def __init__(self):
        self.job = FakeJob(uuid4(), BookingRequest("A-301", "start", "end"))
        self.created = True

    def execute(self, operation, params):
        self.operation = operation
        self.params = params
        return RoomSearchResult((Room("A-301", "А-301", 30, ("projector",)),))

    def book_with_status(self, operation, owner, key, request):
        self.operation, self.owner, self.key = operation, owner, key
        self.job = FakeJob(self.job.id, request)
        created, self.created = self.created, False
        return self.job, created

    def get(self, owner, job_id):
        self.owner = owner
        return self.job

    def cancel(self, owner, job_id):
        self.owner = owner
        self.job.state = JobState.CANCELLED
        return self.job


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.dispatcher = FakeDispatcher()
        self.client = TestClient(create_app(self.dispatcher, {"test-token": "user-1"}))
        self.headers = {"Authorization": "Bearer test-token"}

    def test_search_routes_to_dispatcher(self):
        response = self.client.post("/rooms/search", headers=self.headers, json={
            "start_at": "2026-12-01T10:00:00Z",
            "end_at": "2026-12-01T11:00:00Z",
            "participants": 10,
            "equipment": ["projector"],
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["rooms"][0]["id"], "A-301")
        self.assertEqual(self.dispatcher.operation, "SearchRoomsTool")
        self.assertEqual(response.headers["cache-control"], "no-store")

    def test_create_and_replay_job_use_contract_statuses(self):
        headers = {**self.headers, "Idempotency-Key": "request-1"}
        body = {"room_id": "A-301", "start_at": "start", "end_at": "end"}
        created = self.client.post("/jobs", headers=headers, json=body)
        replay = self.client.post("/jobs", headers=headers, json=body)
        self.assertEqual(created.status_code, 201)
        self.assertEqual(replay.status_code, 200)
        self.assertEqual(created.headers["location"], f"/jobs/{self.dispatcher.job.id}")
        self.assertEqual(created.json()["state"], "confirmed")

    def test_get_job_and_cancel_use_authenticated_owner(self):
        job_id = str(self.dispatcher.job.id)
        self.assertEqual(self.client.get(f"/jobs/{job_id}", headers=self.headers).status_code, 200)
        cancelled = self.client.post(f"/jobs/{job_id}/cancel", headers=self.headers)
        self.assertEqual(cancelled.status_code, 200)
        self.assertEqual(cancelled.json()["state"], "cancelled")
        self.assertEqual(self.dispatcher.owner, "user-1")

    def test_missing_token_is_rejected(self):
        response = self.client.get(f"/jobs/{self.dispatcher.job.id}")
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["code"], "UNAUTHORIZED")


if __name__ == "__main__":
    unittest.main()
