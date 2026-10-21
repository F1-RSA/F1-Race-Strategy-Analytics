"""Postgres sink: upserts OpenF1 records into raw.* on their natural keys."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from f1_ingest.config import Settings

logger = logging.getLogger(__name__)

Record = dict[str, Any]

# Must match the primary keys in db/init/02_raw_tables.sql. meeting_key is a plain column.
KEY_COLUMNS: dict[str, tuple[str, ...]] = {
    "sessions": ("session_key",),
    "drivers": ("session_key", "driver_number"),
    "laps": ("session_key", "driver_number", "lap_number"),
    "stints": ("session_key", "driver_number", "stint_number"),
    "pit": ("session_key", "driver_number", "date"),
}


def connect(settings: Settings) -> psycopg.Connection:
    """Autocommit connection: transactions are opened explicitly where atomicity matters."""
    return psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        user=settings.postgres_user,
        password=settings.postgres_password,
        dbname=settings.postgres_db,
        autocommit=True,
    )


class PostgresSink:
    def __init__(self, conn: psycopg.Connection) -> None:
        self._conn = conn

    def upsert(self, endpoint: str, records: Sequence[Record], run_id: str) -> int:
        """Write all records of one endpoint in one transaction; returns the row count.

        The record is stored unchanged as payload. Key columns come from the record itself;
        a missing key or a duplicate key raises before anything is written, because silently
        skipping or merging rows would hide data loss.
        """
        if endpoint not in KEY_COLUMNS:
            raise ValueError(f"unknown endpoint {endpoint!r}, expected one of {tuple(KEY_COLUMNS)}")
        columns = (*KEY_COLUMNS[endpoint], "meeting_key")
        rows = _rows(endpoint, columns, records, run_id)
        if not rows:
            return 0
        with self._conn.transaction(), self._conn.cursor() as cur:
            cur.executemany(_upsert_sql(endpoint, columns), rows)
        logger.info("raw.%s: upserted %d rows (run %s)", endpoint, len(rows), run_id)
        return len(rows)


def _rows(
    endpoint: str, columns: tuple[str, ...], records: Sequence[Record], run_id: str
) -> list[tuple[Any, ...]]:
    key_width = len(KEY_COLUMNS[endpoint])
    rows: list[tuple[Any, ...]] = []
    seen: set[tuple[Any, ...]] = set()
    for index, record in enumerate(records):
        values = tuple(record.get(column) for column in columns)
        missing = [c for c, v in zip(columns, values, strict=True) if v is None]
        if missing:
            raise ValueError(f"{endpoint} record #{index} has no {', '.join(missing)}: {record}")
        key = values[:key_width]
        if key in seen:
            raise ValueError(f"{endpoint} has more than one record with key {key}")
        seen.add(key)
        rows.append((*values, Jsonb(record), run_id))
    return rows


def _upsert_sql(endpoint: str, columns: tuple[str, ...]) -> str:
    # Table and column names come from the constants above, never from API data.
    key = ", ".join(KEY_COLUMNS[endpoint])
    names = ", ".join((*columns, "payload", "_run_id"))
    placeholders = ", ".join(["%s"] * (len(columns) + 2))
    return (
        f"INSERT INTO raw.{endpoint} ({names}) VALUES ({placeholders}) "
        f"ON CONFLICT ({key}) DO UPDATE SET meeting_key = EXCLUDED.meeting_key, "
        "payload = EXCLUDED.payload, _ingested_at = now(), _run_id = EXCLUDED._run_id"
    )
