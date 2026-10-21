-- Raw layer: natural key columns plus the original JSON record.
-- _ingested_at and _run_id record when and by which run a row was written.
-- Upserts use ON CONFLICT on the primary key, so reruns do not create duplicates.

CREATE TABLE IF NOT EXISTS raw.meetings (
    meeting_key    BIGINT      NOT NULL,
    payload        JSONB       NOT NULL,
    _ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    _run_id        TEXT,
    PRIMARY KEY (meeting_key)
);

CREATE TABLE IF NOT EXISTS raw.sessions (
    session_key    BIGINT      NOT NULL,
    meeting_key    BIGINT      NOT NULL,
    payload        JSONB       NOT NULL,
    _ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    _run_id        TEXT,
    PRIMARY KEY (session_key)
);

CREATE TABLE IF NOT EXISTS raw.drivers (
    session_key    BIGINT      NOT NULL,
    driver_number  INTEGER     NOT NULL,
    meeting_key    BIGINT      NOT NULL,
    payload        JSONB       NOT NULL,
    _ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    _run_id        TEXT,
    PRIMARY KEY (session_key, driver_number)
);

CREATE TABLE IF NOT EXISTS raw.laps (
    session_key    BIGINT      NOT NULL,
    driver_number  INTEGER     NOT NULL,
    lap_number     INTEGER     NOT NULL,
    meeting_key    BIGINT      NOT NULL,
    payload        JSONB       NOT NULL,
    _ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    _run_id        TEXT,
    PRIMARY KEY (session_key, driver_number, lap_number)
);

CREATE TABLE IF NOT EXISTS raw.stints (
    session_key    BIGINT      NOT NULL,
    driver_number  INTEGER     NOT NULL,
    stint_number   INTEGER     NOT NULL,
    meeting_key    BIGINT      NOT NULL,
    payload        JSONB       NOT NULL,
    _ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    _run_id        TEXT,
    PRIMARY KEY (session_key, driver_number, stint_number)
);

-- A pit stop has no number of its own. The entry timestamp identifies it
-- even if a driver enters the pit lane twice on one lap.
CREATE TABLE IF NOT EXISTS raw.pit (
    session_key    BIGINT      NOT NULL,
    driver_number  INTEGER     NOT NULL,
    date           TIMESTAMPTZ NOT NULL,
    meeting_key    BIGINT      NOT NULL,
    payload        JSONB       NOT NULL,
    _ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    _run_id        TEXT,
    PRIMARY KEY (session_key, driver_number, date)
);
