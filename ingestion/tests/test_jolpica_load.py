import copy
from datetime import UTC, datetime
from typing import Any

import httpx
import psycopg
import pytest
from test_jolpica import ALL_RACES, RESULT_RACES, FakeJolpica

from f1_ingest.http import RetriesExhaustedError
from f1_ingest.jolpica_load import SeasonResult, race_start, result_rows, run_season
from f1_ingest.openf1 import ENDPOINTS
from f1_ingest.sinks.postgres import PostgresSink
from f1_ingest.state import LoadLog

# The fixtures hold results for rounds 1-3 only (races 2024-03-02, -03-09, -03-24).
AFTER_ROUND_3 = datetime(2024, 3, 25, tzinfo=UTC)
AFTER_ROUND_2 = datetime(2024, 3, 10, tzinfo=UTC)
AFTER_ROUND_4 = datetime(2024, 4, 8, tzinfo=UTC)  # round 4 is over, but has no results here
BEFORE_SEASON = datetime(2024, 1, 1, tzinfo=UTC)


def test_race_start_reads_date_and_time_as_utc() -> None:
    assert race_start({"date": "2024-03-02", "time": "15:00:00Z"}) == datetime(
        2024, 3, 2, 15, tzinfo=UTC
    )


def test_race_without_time_counts_from_the_next_day() -> None:
    assert race_start({"date": "1950-05-13"}) == datetime(1950, 5, 14, tzinfo=UTC)


def test_result_rows_split_a_race_into_one_row_per_result() -> None:
    race = RESULT_RACES[0]
    rows = result_rows(2024, race)
    assert len(rows) == len(race["Results"]) == 20
    first = rows[0]
    assert (first["season"], first["round"], first["driver_id"]) == (2024, 1, "max_verstappen")
    payload: dict[str, Any] = first["payload"]  # type: ignore[assignment]
    assert payload["Results"] == [race["Results"][0]]
    assert {k: v for k, v in payload.items() if k != "Results"} == {
        k: v for k, v in race.items() if k != "Results"
    }
    assert payload["Results"][0]["position"] == "1"  # still a string


def test_result_without_driver_id_fails_before_writing() -> None:
    race = copy.deepcopy(RESULT_RACES[0])
    del race["Results"][3]["Driver"]
    with pytest.raises(ValueError, match="driver_id"):
        PostgresSink(None).upsert_rows(  # type: ignore[arg-type]
            "jolpica_results", ("season", "round", "driver_id"), result_rows(2024, race), "run"
        )


def test_two_results_for_one_driver_fail_instead_of_merging() -> None:
    race = copy.deepcopy(RESULT_RACES[0])
    race["Results"][1]["Driver"]["driverId"] = race["Results"][0]["Driver"]["driverId"]
    with pytest.raises(ValueError, match="more than one row"):
        PostgresSink(None).upsert_rows(  # type: ignore[arg-type]
            "jolpica_results", ("season", "round", "driver_id"), result_rows(2024, race), "run"
        )


def test_upsert_rows_refuses_names_that_are_not_identifiers() -> None:
    with pytest.raises(ValueError, match="invalid table"):
        PostgresSink(None).upsert_rows("raw; DROP TABLE x", ("a",), [], "run")  # type: ignore[arg-type]


def do_run(
    api: FakeJolpica,
    conn: psycopg.Connection,
    now: datetime = AFTER_ROUND_3,
    mode: str = "incremental",
    page_size: int = 100,
) -> SeasonResult:
    with api.client(page_size, max_retries=2) as client:
        return run_season(2024, client=client, conn=conn, mode=mode, now=now)  # type: ignore[arg-type]


def one(conn: psycopg.Connection, sql: str, *params: Any) -> Any:
    row = conn.execute(sql, params).fetchone()
    assert row is not None
    return row[0]


def per_round(conn: psycopg.Connection) -> list[tuple[int, int]]:
    return conn.execute(
        "SELECT round, count(*) FROM raw.jolpica_results GROUP BY round ORDER BY round"
    ).fetchall()


def results_log(conn: psycopg.Connection) -> list[tuple[str, str, int | None]]:
    return conn.execute(
        "SELECT endpoint, status, row_count FROM meta.load_log "
        "WHERE source = 'jolpica' AND endpoint LIKE 'results:%' ORDER BY log_id"
    ).fetchall()


@pytest.mark.integration
def test_run_loads_the_schedule_and_the_results_of_completed_rounds(
    conn: psycopg.Connection,
) -> None:
    result = do_run(FakeJolpica(), conn)
    assert result.ok
    assert result.loaded == [1, 2, 3]
    assert result.races == one(conn, "SELECT count(*) FROM raw.jolpica_races") == 24
    assert per_round(conn) == [(1, 20), (2, 20), (3, 19)]
    assert one(conn, "SELECT count(*) FROM raw.jolpica_results") == 59
    assert results_log(conn) == [
        ("results:2024", "success", 59),
        ("results:2024:1", "success", 20),
        ("results:2024:2", "success", 20),
        ("results:2024:3", "success", 19),
    ]
    assert conn.execute(
        "SELECT status, row_count FROM meta.load_log WHERE endpoint = 'races:2024'"
    ).fetchall() == [("success", 24)]


@pytest.mark.integration
def test_payload_is_stored_unchanged(conn: psycopg.Connection) -> None:
    do_run(FakeJolpica(), conn)
    race = ALL_RACES[0]
    assert one(conn, "SELECT payload FROM raw.jolpica_races WHERE round = 1") == race
    stored = one(
        conn,
        "SELECT payload FROM raw.jolpica_results WHERE round = 1 AND driver_id = 'max_verstappen'",
    )
    assert stored["Results"] == [RESULT_RACES[0]["Results"][0]]
    assert (stored["season"], stored["round"]) == ("2024", "1")


@pytest.mark.integration
@pytest.mark.parametrize("page_size", [25, 7, 1])
def test_page_border_inside_a_round_loads_the_same_rows(
    conn: psycopg.Connection, page_size: int
) -> None:
    do_run(FakeJolpica(), conn, page_size=page_size)
    assert per_round(conn) == [(1, 20), (2, 20), (3, 19)]
    assert one(conn, "SELECT count(DISTINCT (round, driver_id)) FROM raw.jolpica_results") == 59


@pytest.mark.integration
def test_incremental_rerun_requests_only_the_schedule(conn: psycopg.Connection) -> None:
    do_run(FakeJolpica(), conn)
    api = FakeJolpica()
    result = do_run(api, conn)
    assert result.loaded == []
    assert result.already_loaded == [1, 2, 3]
    assert [r.url.path.rsplit("/", 1)[-1] for r in api.requests] == ["races.json"]
    assert one(conn, "SELECT count(*) FROM raw.jolpica_results") == 59


@pytest.mark.integration
def test_full_rerun_rewrites_everything_and_keeps_the_counts(conn: psycopg.Connection) -> None:
    do_run(FakeJolpica(), conn)
    first = one(conn, "SELECT max(_ingested_at) FROM raw.jolpica_results")
    result = do_run(FakeJolpica(), conn, mode="full")
    assert result.loaded == [1, 2, 3]
    assert per_round(conn) == [(1, 20), (2, 20), (3, 19)]
    assert one(conn, "SELECT count(*) FROM raw.jolpica_races") == 24
    assert one(conn, "SELECT min(_ingested_at) FROM raw.jolpica_results") > first


@pytest.mark.integration
def test_only_completed_rounds_are_loaded_and_a_later_run_adds_the_rest(
    conn: psycopg.Connection,
) -> None:
    early = do_run(FakeJolpica(), conn, now=AFTER_ROUND_2)
    assert early.loaded == [1, 2]
    assert early.upcoming == list(range(3, 25))
    assert per_round(conn) == [(1, 20), (2, 20)]
    later = do_run(FakeJolpica(), conn, now=AFTER_ROUND_3)
    assert later.loaded == [3]
    assert later.already_loaded == [1, 2]
    assert per_round(conn) == [(1, 20), (2, 20), (3, 19)]


@pytest.mark.integration
def test_before_the_season_loads_the_schedule_and_requests_no_results(
    conn: psycopg.Connection,
) -> None:
    api = FakeJolpica()
    result = do_run(api, conn, now=BEFORE_SEASON)
    assert result.loaded == [] and result.races == 24
    assert len(api.requests) == 1
    assert one(conn, "SELECT count(*) FROM raw.jolpica_results") == 0


@pytest.mark.integration
def test_round_without_published_results_is_pending_and_picked_up_later(
    conn: psycopg.Connection,
) -> None:
    result = do_run(FakeJolpica(), conn, now=AFTER_ROUND_4)
    assert result.ok
    assert result.loaded == [1, 2, 3]
    assert result.pending == [4]
    assert "results:2024:4" not in LoadLog(conn, "jolpica").succeeded_endpoints("results:2024:")
    # Still pending on the next run: it is requested again, never marked loaded.
    assert do_run(FakeJolpica(), conn, now=AFTER_ROUND_4).pending == [4]


@pytest.mark.integration
def test_running_row_of_a_killed_run_is_closed_and_the_round_is_picked_up(
    conn: psycopg.Connection,
) -> None:
    LoadLog(conn, "jolpica").start("killed-run", "results:2024:1")
    result = do_run(FakeJolpica(), conn)
    assert result.loaded == [1, 2, 3]
    assert conn.execute(
        "SELECT status FROM meta.load_log WHERE run_id = 'killed-run'"
    ).fetchall() == [("failed",)]
    assert one(conn, "SELECT count(*) FROM meta.load_log WHERE status = 'running'") == 0


def same_driver_twice_in_round_2(payload: dict[str, Any]) -> dict[str, Any]:
    results = payload["MRData"]["RaceTable"]["Races"][1]["Results"]
    results[1]["Driver"]["driverId"] = results[0]["Driver"]["driverId"]
    return payload


@pytest.mark.integration
def test_failing_round_writes_nothing_and_the_other_rounds_still_load(
    conn: psycopg.Connection,
) -> None:
    result = do_run(FakeJolpica(tamper={1: same_driver_twice_in_round_2}), conn)
    assert not result.ok
    assert list(result.failed) == [2]
    assert "more than one row" in result.failed[2]
    assert result.loaded == [1, 3]
    assert per_round(conn) == [(1, 20), (3, 19)]
    assert ("results:2024:2", "failed", None) in results_log(conn)
    # The next run (data fixed) loads exactly the missing round.
    again = do_run(FakeJolpica(), conn)
    assert again.loaded == [2]
    assert per_round(conn) == [(1, 20), (2, 20), (3, 19)]


@pytest.mark.integration
def test_failed_results_fetch_is_logged_and_raised(conn: psycopg.Connection) -> None:
    api = FakeJolpica(tamper={n: lambda _: httpx.Response(503) for n in (1, 2, 3)})
    with pytest.raises(RetriesExhaustedError):
        do_run(api, conn)
    assert one(conn, "SELECT count(*) FROM raw.jolpica_results") == 0
    assert results_log(conn) == [("results:2024", "failed", None)]
    assert one(conn, "SELECT count(*) FROM raw.jolpica_races") == 24  # the schedule was kept


@pytest.mark.integration
def test_unknown_season_fails_instead_of_loading_nothing(conn: psycopg.Connection) -> None:
    with FakeJolpica().client() as client, pytest.raises(LookupError, match="no races"):
        run_season(1900, client=client, conn=conn, mode="incremental")
    assert conn.execute(
        "SELECT status FROM meta.load_log WHERE endpoint = 'races:1900'"
    ).fetchall() == [("failed",)]


@pytest.mark.integration
def test_jolpica_and_openf1_watermarks_do_not_see_each_other(conn: psycopg.Connection) -> None:
    do_run(FakeJolpica(), conn)
    openf1_log = LoadLog(conn)
    assert openf1_log.loaded_session_keys(ENDPOINTS) == set()
    for endpoint in ENDPOINTS:
        log_id = openf1_log.start("run", endpoint, 9472)
        openf1_log.finish(log_id, 1)
    assert openf1_log.loaded_session_keys(ENDPOINTS) == {9472}
    assert openf1_log.succeeded_endpoints("results:") == set()
    assert LoadLog(conn, "jolpica").succeeded_endpoints("results:2024:") == {
        "results:2024:1",
        "results:2024:2",
        "results:2024:3",
    }
    assert LoadLog(conn, "jolpica").succeeded_endpoints("laps") == set()
