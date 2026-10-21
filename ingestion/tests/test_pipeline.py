import json
from pathlib import Path
from typing import Any

import httpx
import psycopg
import pytest
from test_openf1 import FakeOpenF1

from f1_ingest.openf1 import ENDPOINTS
from f1_ingest.pipeline import run, select_sessions
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


def test_selection_is_in_date_order_and_filtered_by_type() -> None:
    chosen, skipped = select_sessions(
        SEASON, loaded=set(), mode="incremental", session_types={"Race"}
    )
    assert keys(chosen) == [1, 3, 4, 5]
    assert skipped == []


def test_no_type_filter_keeps_every_session() -> None:
    chosen, _ = select_sessions(SEASON, loaded=set(), mode="full", session_types=None)
    assert keys(chosen) == [1, 2, 3, 4, 5]


def test_incremental_skips_loaded_sessions() -> None:
    chosen, skipped = select_sessions(
        SEASON, loaded={1, 3}, mode="incremental", session_types={"Race"}
    )
    assert keys(chosen) == [4, 5]
    assert skipped == [1, 3]


def test_full_reloads_loaded_sessions() -> None:
    chosen, skipped = select_sessions(SEASON, loaded={1, 3}, mode="full", session_types={"Race"})
    assert keys(chosen) == [1, 3, 4, 5]
    assert skipped == []


def test_meeting_limit_takes_the_next_meetings_not_yet_loaded() -> None:
    first, _ = select_sessions(
        SEASON, loaded=set(), mode="incremental", session_types={"Race"}, meeting_limit=1
    )
    assert keys(first) == [1]
    after_first, _ = select_sessions(
        SEASON, loaded={1}, mode="incremental", session_types={"Race"}, meeting_limit=1
    )
    assert keys(after_first) == [3, 4]


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
