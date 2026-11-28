CREATE TABLE rooms (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    capacity INTEGER NOT NULL,
    equipment TEXT NOT NULL
);

CREATE TABLE jobs (
    id TEXT PRIMARY KEY NOT NULL,
    owner_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    room_id TEXT NOT NULL,
    request_start_at TEXT NOT NULL,
    request_end_at TEXT NOT NULL,
    state TEXT NOT NULL,
    interval_start TEXT,
    interval_end TEXT,
    outcome_error TEXT,
    UNIQUE (owner_id, idempotency_key)
);

CREATE INDEX jobs_confirmed_room_interval
    ON jobs (room_id, interval_end, interval_start)
    WHERE state = 'confirmed';
