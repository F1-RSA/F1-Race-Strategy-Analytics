"""Selects sessions and loads them endpoint by endpoint, recording every attempt."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Collection
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal

import psycopg

from f1_ingest.openf1 import ENDPOINTS, OpenF1Client, Record
from f1_ingest.sinks.postgres import PostgresSink
from f1_ingest.state import LoadLog

logger = logging.getLogger(__name__)

Mode = Literal["full", "incremental"]


@dataclass
class RunResult:
    run_id: str
    loaded: list[int] = field(default_factory=list)
    already_loaded: list[int] = field(default_factory=list)
    not_ready: dict[int, str] = field(default_factory=dict)
    failed: dict[int, str] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.failed


def new_run_id() -> str:
    return f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


@dataclass
class Selection:
    to_load: list[Record]
    already_loaded: list[int]
    not_ready: dict[int, str]  # session_key -> why it is not loaded yet


def not_ready_reason(session: Record, now: datetime) -> str | None:
    """Why a session must not be loaded yet, None if it is finished.

    OpenF1 can restrict or change data while a session is live, and a laps/stints/pit set
    loaded mid-session would be marked loaded and never refreshed by incremental runs.
    """
    if session.get("is_cancelled"):
        return "cancelled"
    date_end = session.get("date_end")
    if not date_end:
        return "has no date_end"
    end = datetime.fromisoformat(date_end)
    if end.tzinfo is None:
        end = end.replace(tzinfo=UTC)
    if end > now:
        return f"not finished yet (ends {date_end})"
    return None


def select_sessions(
    sessions: list[Record],
    *,
    loaded: set[int],
    mode: Mode,
    session_types: Collection[str] | None,
    now: datetime,
    meeting_limit: int | None = None,
) -> Selection:
    """Sessions to load in date order, plus those skipped as loaded or not ready.

    Incremental = listed sessions minus loaded ones; full = everything listed. The meeting
    limit counts weekends among the sessions that would be loaded, so it means "the next N".
    """
    wanted = [s for s in sessions if session_types is None or s["session_type"] in session_types]
    wanted.sort(key=lambda s: s["date_start"])
    not_ready = {
        s["session_key"]: reason for s in wanted if (reason := not_ready_reason(s, now)) is not None
    }
    wanted = [s for s in wanted if s["session_key"] not in not_ready]
    skipped: list[int] = []
    if mode == "incremental":
        skipped = [s["session_key"] for s in wanted if s["session_key"] in loaded]
        wanted = [s for s in wanted if s["session_key"] not in loaded]
    if meeting_limit is not None:
        meetings = list(dict.fromkeys(s["meeting_key"] for s in wanted))[:meeting_limit]
        wanted = [s for s in wanted if s["meeting_key"] in meetings]
    return Selection(wanted, skipped, not_ready)


def load_session(
    session: Record,
    *,
    client: OpenF1Client,
    sink: PostgresSink,
    log: LoadLog,
    conn: psycopg.Connection,
    run_id: str,
) -> None:
    """Load the five endpoints of one session; raises on the first failing endpoint.

    The `sessions` record was already fetched when the sessions were listed, so it is
    written from there instead of being requested a second time. A session is loaded only
    when all five endpoints are logged as success, so stopping here leaves it "not loaded".
    """
    session_key = session["session_key"]
    for endpoint in ENDPOINTS:
        log_id = log.start(run_id, endpoint, session_key)
        try:
            records = [session] if endpoint == "sessions" else client.fetch(endpoint, session_key)
            # Data and `success` commit together: a success row proves the rows are there.
            with conn.transaction():
                row_count = sink.upsert(endpoint, records, run_id)
                log.finish(log_id, row_count)
        except Exception:
            log.fail(log_id)
            raise


def run(
    sessions: list[Record],
    *,
    client: OpenF1Client,
    conn: psycopg.Connection,
    mode: Mode,
    session_types: Collection[str] | None,
    meeting_limit: int | None = None,
    run_id: str | None = None,
    now: datetime | None = None,
) -> RunResult:
    """Load the selected sessions one after the other; a failed session does not stop the rest."""
    result = RunResult(run_id or new_run_id())
    log = LoadLog(conn)
    sink = PostgresSink(conn)

    abandoned = log.abandon_stale_running(result.run_id)
    if abandoned:
        logger.warning("marked %d unfinished log rows of earlier runs as failed", abandoned)

    selection = select_sessions(
        sessions,
        loaded=log.loaded_session_keys(ENDPOINTS),
        mode=mode,
        session_types=session_types,
        now=now or datetime.now(UTC),
        meeting_limit=meeting_limit,
    )
    to_load = selection.to_load
    result.already_loaded = selection.already_loaded
    result.not_ready = selection.not_ready
    for key, reason in result.not_ready.items():
        logger.info("session %s skipped: %s", key, reason)
    logger.info(
        "run %s: %d sessions to load, %d already loaded, %d not ready",
        result.run_id,
        len(to_load),
        len(result.already_loaded),
        len(result.not_ready),
    )
    for session in to_load:
        key = session["session_key"]
        try:
            load_session(
                session, client=client, sink=sink, log=log, conn=conn, run_id=result.run_id
            )
        except Exception as exc:
            logger.exception("session %s failed", key)
            result.failed[key] = f"{type(exc).__name__}: {exc}"
        else:
            result.loaded.append(key)
            logger.info("session %s loaded", key)
    return result
