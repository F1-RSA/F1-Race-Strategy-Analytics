-- Jolpica (Ergast-compatible) raw tables, same pattern as the OpenF1 tables: key columns plus
-- the original JSON, written by upserts on the primary key. Jolpica has no meeting_key; a race
-- is identified by season and round. Both arrive as strings in the API and are stored as integers.

CREATE TABLE IF NOT EXISTS raw.jolpica_races (
    season         INTEGER     NOT NULL,
    round          INTEGER     NOT NULL,
    payload        JSONB       NOT NULL,
    _ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    _run_id        TEXT,
    PRIMARY KEY (season, round)
);

-- One row per result. The payload is the race object as the API sends it, with "Results"
-- reduced to this one result. driver_id is Jolpica's own driverId (e.g. 'max_verstappen'),
-- not the position, which can change after a post-race penalty.
CREATE TABLE IF NOT EXISTS raw.jolpica_results (
    season         INTEGER     NOT NULL,
    round          INTEGER     NOT NULL,
    driver_id      TEXT        NOT NULL,
    payload        JSONB       NOT NULL,
    _ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    _run_id        TEXT,
    PRIMARY KEY (season, round, driver_id)
);
