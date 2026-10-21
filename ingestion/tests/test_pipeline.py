import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import psycopg
import pytest
from test_openf1 import FakeOpenF1

from f1_ingest.openf1 import ENDPOINTS
from f1_ingest.pipeline import not_ready_reason, run, select_sessions
from f1_ingest.state import LoadLog

FIXTURES = Path(__file__).parent / "fixtures" / "openf1" / "session_9472"
EXPECTED_COUNTS = {"sessions": 1, "drivers": 20, "laps": 114, "stints": 63, "pit": 43}
RACE = json.loads((FIXTURES / "sessions.json").read_text())[0]


def session(key: int, meeting: int, start: str, kind: str = "Race") -> dict[str, Any]:
    return {
        "session_key": key,
        "meeting_key": meeting,
        "session_type": kind,
        "date_start": start,
        "date_end": start,
    }


SEASON = [
    session(5, 3, "2024-03-30"),
    session(1, 1, "2024-03-01"),
    session(2, 1, "2024-03-02", "Qualifying"),
    session(3, 2, "2024-03-16"),
    session(4, 2, "2024-03-17"),
]


def keys(sessions: list[dict[str, Any]]) -> list[int]:
    return [s["session_key"] for s in sessions]


NOW = datetime(2024, 12, 1, tzinfo=UTC)


def pick(
    sessions: list[dict[str, Any]] = SEASON, **kwargs: Any
) -> tuple[list[dict[str, Any]], list[int]]:
    kwargs = {
        "loaded": set(),
        "mode": "incremental",
        "session_types": {"Race"},
        "now": NOW,
    } | kwargs
    selection = select_sessions(sessions, **kwargs)
    return selection.to_load, selection.already_loaded


def test_selection_is_in_date_order_and_filtered_by_type() -> None:
    chosen, skipped = pick()
    assert keys(chosen) == [1, 3, 4, 5]
    assert skipped == []


def test_no_type_filter_keeps_every_session() -> None:
    chosen, _ = pick(mode="full", session_types=None)
    assert keys(chosen) == [1, 2, 3, 4, 5]


def test_incremental_skips_loaded_sessions() -> None:
    chosen, skipped = pick(loaded={1, 3})
    assert keys(chosen) == [4, 5]
    assert skipped == [1, 3]


def test_full_reloads_loaded_sessions() -> None:
    chosen, skipped = pick(loaded={1, 3}, mode="full")
    assert keys(chosen) == [1, 3, 4, 5]
    assert skipped == []


def test_meeting_limit_keeps_the_first_weekends_so_a_repeat_is_a_noop() -> None:
    first, _ = pick(meeting_limit=1)
    assert keys(first) == [1]
    repeat, skipped = pick(loaded={1}, meeting_limit=1)
    assert repeat == [] and skipped == [1]


def test_larger_meeting_limit_adds_only_the_new_weekend() -> None:
    chosen, skipped = pick(loaded={1}, meeting_limit=2)
    assert keys(chosen) == [3, 4]
    assert skipped == [1]


def ending(key: int, meeting: int, date_end: str | None, **extra: Any) -> dict[str, Any]:
    return session(key, meeting, "2024-11-30") | {"date_end": date_end} | extra


def test_unfinished_sessions_are_skipped_with_a_reason() -> None:
    live = ending(10, 7, "2024-12-01T00:00:01+00:00")
    selection = select_sessions(
        [*SEASON, live], loaded=set(), mode="full", session_types={"Race"}, now=NOW
    )
    assert 10 not in keys(selection.to_load)
    assert "not finished yet" in selection.not_ready[10]


def test_session_ending_exactly_now_counts_as_finished() -> None:
    assert not_ready_reason(ending(10, 7, "2024-12-01T00:00:00+00:00"), NOW) is None


@pytest.mark.parametrize(
    ("record", "reason"),
    [
        (ending(10, 7, "2024-11-30T10:00:00+00:00", is_cancelled=True), "cancelled"),
        (ending(10, 7, None), "no date_end"),
        (ending(10, 7, "2024-12-01T01:00:00+01:00"), None),  # 00:00 UTC, offset is honoured
        (ending(10, 7, "2024-12-01T01:00:00Z"), "not finished"),
        (ending(10, 7, "2024-11-30T10:00:00"), None),  # no offset: read as UTC
    ],
)
def test_not_ready_reason(record: dict[str, Any], reason: str | None) -> None:
    found = not_ready_reason(record, NOW)
    assert (found is None) if reason is None else (reason in (found or ""))


def test_unfinished_session_does_not_use_up_the_meeting_limit() -> None:
    live = ending(10, 7, "2999-01-01T00:00:00+00:00") | {"date_start": "2024-02-01"}
    chosen, _ = pick([*SEASON, live], meeting_limit=1)
    assert keys(chosen) == [1]


def counts(conn: psycopg.Connection) -> dict[str, int]:
    return {
        e: conn.execute(f"SELECT count(*) FROM raw.{e}").fetchone()[0]  # type: ignore[index]
        for e in ENDPOINTS
    }


def log_rows(conn: psycopg.Connection, key: int = 9472) -> list[tuple[str, str, int | None]]:
    return conn.execute(
        "SELECT endpoint, status, row_count FROM meta.load_log WHERE session_key = %s "
        "ORDER BY log_id",
        (key,),
    ).fetchall()


def do_run(api: FakeOpenF1, conn: psycopg.Connection, sessions=None, mode="incremental", **kw):
    with api.client(max_retries=2) as client:
        return run(
            sessions or [RACE],
            client=client,
            conn=conn,
            mode=mode,
            session_types={"Race"},
            **kw,
        )


@pytest.mark.integration
def test_run_logs_one_success_row_per_endpoint(conn: psycopg.Connection) -> None:
    result = do_run(FakeOpenF1(), conn)
    assert result.ok and result.loaded == [9472]
    assert log_rows(conn) == [(e, "success", EXPECTED_COUNTS[e]) for e in ENDPOINTS]
    assert counts(conn) == EXPECTED_COUNTS
    assert LoadLog(conn).loaded_session_keys(ENDPOINTS) == {9472}


@pytest.mark.integration
def test_incremental_rerun_skips_the_loaded_session_without_requests(
    conn: psycopg.Connection,
) -> None:
    do_run(FakeOpenF1(), conn)
    api = FakeOpenF1()
    result = do_run(api, conn)
    assert result.already_loaded == [9472] and result.loaded == []
    assert api.requests == []
    assert len(log_rows(conn)) == 5


@pytest.mark.integration
def test_full_rerun_reloads_and_keeps_row_counts(conn: psycopg.Connection) -> None:
    do_run(FakeOpenF1(), conn)
    api = FakeOpenF1()
    result = do_run(api, conn, mode="full")
    assert result.loaded == [9472]
    assert counts(conn) == EXPECTED_COUNTS
    assert len(api.requests) == 4  # the sessions record comes from the listing
    assert len(log_rows(conn)) == 10


@pytest.mark.integration
def test_incremental_loads_only_the_new_session(conn: psycopg.Connection) -> None:
    do_run(FakeOpenF1(), conn)
    new = session(9999, 2000, "2024-04-01")
    api = FakeOpenF1()
    result = do_run(api, conn, sessions=[RACE, new])
    assert result.loaded == [9999] and result.already_loaded == [9472]
    assert {r.url.params["session_key"] for r in api.requests} == {"9999"}


@pytest.mark.integration
def test_failed_endpoint_leaves_session_not_loaded(conn: psycopg.Connection) -> None:
    api = FakeOpenF1({"stints": [httpx.Response(503)] * 3})
    result = do_run(api, conn)
    assert list(result.failed) == [9472] and result.loaded == []
    assert log_rows(conn) == [
        ("sessions", "success", 1),
        ("drivers", "success", 20),
        ("laps", "success", 114),
        ("stints", "failed", None),
    ]
    assert LoadLog(conn).loaded_session_keys(ENDPOINTS) == set()


@pytest.mark.integration
def test_rerun_after_failure_completes_the_session(conn: psycopg.Connection) -> None:
    do_run(FakeOpenF1({"stints": [httpx.Response(503)] * 3}), conn)
    result = do_run(FakeOpenF1(), conn)
    assert result.loaded == [9472]
    assert counts(conn) == EXPECTED_COUNTS
    assert LoadLog(conn).loaded_session_keys(ENDPOINTS) == {9472}


@pytest.mark.integration
def test_running_row_of_a_killed_run_is_closed_and_session_is_picked_up(
    conn: psycopg.Connection,
) -> None:
    # What a SIGKILL after "start" leaves behind: a row that never gets finished.
    LoadLog(conn).start("killed-run", "laps", 9472)
    result = do_run(FakeOpenF1(), conn)
    assert result.loaded == [9472]
    stale = conn.execute(
        "SELECT status, finished_at IS NOT NULL FROM meta.load_log WHERE run_id = 'killed-run'"
    ).fetchall()
    assert stale == [("failed", True)]
    assert conn.execute(
        "SELECT count(*) FROM meta.load_log WHERE status = 'running'"
    ).fetchone() == (0,)


@pytest.mark.integration
def test_record_without_key_fails_the_endpoint_and_writes_nothing(
    conn: psycopg.Connection,
) -> None:
    bad = [{"session_key": 9472, "meeting_key": 1229, "driver_number": 1}]
    result = do_run(FakeOpenF1({"laps": [httpx.Response(200, json=bad)]}), conn)
    assert "lap_number" in result.failed[9472]
    assert counts(conn)["laps"] == 0
    assert ("laps", "failed", None) in log_rows(conn)


@pytest.mark.integration
def test_one_failed_session_does_not_stop_the_others(conn: psycopg.Connection) -> None:
    new = session(9999, 2000, "2024-04-01")
    api = FakeOpenF1({"drivers": [httpx.Response(503)] * 3})
    result = do_run(api, conn, sessions=[RACE, new])
    assert list(result.failed) == [9472]
    assert result.loaded == [9999]


@pytest.mark.integration
def test_loaded_needs_all_five_endpoints(conn: psycopg.Connection) -> None:
    log = LoadLog(conn)
    for endpoint in ENDPOINTS[:-1]:
        log.finish(log.start("r", endpoint, 7), 1)
    assert log.loaded_session_keys(ENDPOINTS) == set()
    log.finish(log.start("r", ENDPOINTS[-1], 7), 0)
    assert log.loaded_session_keys(ENDPOINTS) == {7}


@pytest.mark.integration
def test_unfinished_session_is_neither_requested_nor_logged(conn: psycopg.Connection) -> None:
    live = session(9999, 2000, "2999-01-01") | {"date_end": "2999-01-01T02:00:00+00:00"}
    api = FakeOpenF1()
    result = do_run(api, conn, sessions=[RACE, live])
    assert result.loaded == [9472] and list(result.not_ready) == [9999]
    assert {r.url.params["session_key"] for r in api.requests} == {"9472"}
    assert log_rows(conn, 9999) == []
    assert conn.execute(
        "SELECT count(*) FROM raw.sessions WHERE session_key = 9999"
    ).fetchone() == (0,)


@pytest.mark.integration
def test_injected_clock_decides_what_is_finished(conn: psycopg.Connection) -> None:
    before_end = datetime(2024, 3, 2, 16, 59, tzinfo=UTC)  # RACE ends 17:00
    result = do_run(FakeOpenF1(), conn, now=before_end)
    assert result.loaded == [] and list(result.not_ready) == [9472]
    result = do_run(FakeOpenF1(), conn, now=datetime(2024, 3, 2, 17, 0, tzinfo=UTC))
    assert result.loaded == [9472]
