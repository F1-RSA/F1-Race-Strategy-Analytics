-- One row per (run, source, endpoint, session) attempt. A session counts as
-- loaded when every endpoint has a row with status 'success'. Incremental
-- loads select sessions from the API minus the ones loaded this way.
CREATE TABLE IF NOT EXISTS meta.load_log (
    log_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id       TEXT        NOT NULL,
    source       TEXT        NOT NULL,   -- 'openf1' or 'jolpica'
    endpoint     TEXT        NOT NULL,   -- e.g. 'laps', 'stints'
    session_key  BIGINT,                 -- NULL for endpoints that are not per session
    status       TEXT        NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    row_count    INTEGER,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS load_log_lookup_idx
    ON meta.load_log (source, endpoint, session_key, status);
