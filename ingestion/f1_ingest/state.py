"""meta.load_log: which endpoint of which session was loaded, and how the attempt ended."""

from __future__ import annotations

from collections.abc import Sequence

import psycopg


class LoadLog:
    """One row per attempt, never deleted: the history shows every try, retries included.

    `start` runs on an autocommit connection and is visible at once, so a killed process
    leaves a `running` row behind. `finish` is meant to run in the same transaction as the
    data write, so a `success` row always means the data was committed.
    """

    def __init__(self, conn: psycopg.Connection, source: str = "openf1") -> None:
        self._conn = conn
        self._source = source

    def start(self, run_id: str, endpoint: str, session_key: int) -> int:
        row = self._conn.execute(
            "INSERT INTO meta.load_log (run_id, source, endpoint, session_key, status) "
            "VALUES (%s, %s, %s, %s, 'running') RETURNING log_id",
            (run_id, self._source, endpoint, session_key),
        ).fetchone()
        assert row is not None
        return int(row[0])

    def finish(self, log_id: int, row_count: int) -> None:
        self._end(log_id, "success", row_count)

    def fail(self, log_id: int) -> None:
        self._end(log_id, "failed", None)

    def _end(self, log_id: int, status: str, row_count: int | None) -> None:
        self._conn.execute(
            "UPDATE meta.load_log SET status = %s, row_count = %s, finished_at = now() "
            "WHERE log_id = %s",
            (status, row_count, log_id),
        )

    def loaded_session_keys(self, endpoints: Sequence[str]) -> set[int]:
        """Sessions with a `success` row for every endpoint (the watermark rule)."""
        rows = self._conn.execute(
            "SELECT session_key FROM meta.load_log "
            "WHERE source = %s AND status = 'success' AND endpoint = ANY(%s) "
            "AND session_key IS NOT NULL "
            "GROUP BY session_key HAVING count(DISTINCT endpoint) = %s",
            (self._source, list(endpoints), len(endpoints)),
        ).fetchall()
        return {int(row[0]) for row in rows}

    def abandon_stale_running(self, current_run_id: str) -> int:
        """Mark `running` rows of earlier runs as `failed`; they belong to killed processes.

        Only valid because one run at a time is allowed (Kestra concurrency 1): otherwise
        this would also close the rows of a run that is still working.
        """
        cur = self._conn.execute(
            "UPDATE meta.load_log SET status = 'failed', finished_at = now() "
            "WHERE source = %s AND status = 'running' AND run_id <> %s",
            (self._source, current_run_id),
        )
        return cur.rowcount
