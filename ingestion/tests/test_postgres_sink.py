import copy
import json
from pathlib import Path
from typing import Any

import psycopg
import pytest

from f1_ingest.openf1 import ENDPOINTS
from f1_ingest.sinks.postgres import KEY_COLUMNS, PostgresSink, _rows

FIXTURES = Path(__file__).parent / "fixtures" / "openf1" / "session_9472"
EXPECTED_COUNTS = {"sessions": 1, "drivers": 20, "laps": 114, "stints": 63, "pit": 43}


def records(endpoint: str) -> list[dict[str, Any]]:
    return json.loads((FIXTURES / f"{endpoint}.json").read_text())


def load_all(sink: PostgresSink, run_id: str) -> dict[str, int]:
    return {endpoint: sink.upsert(endpoint, records(endpoint), run_id) for endpoint in ENDPOINTS}


def counts(conn: psycopg.Connection) -> dict[str, int]:
    return {
        endpoint: conn.execute(f"SELECT count(*) FROM raw.{endpoint}").fetchone()[0]  # type: ignore[index]
        for endpoint in ENDPOINTS
    }


def test_key_columns_cover_every_endpoint() -> None:
    assert list(KEY_COLUMNS) == list(ENDPOINTS)


@pytest.mark.parametrize("endpoint", ENDPOINTS)
def test_fixture_records_have_all_key_fields(endpoint: str) -> None:
    columns = (*KEY_COLUMNS[endpoint], "meeting_key")
    rows = _rows(endpoint, columns, records(endpoint), "run")
    assert len(rows) == EXPECTED_COUNTS[endpoint]


def test_missing_key_field_fails_loudly() -> None:
    laps = records("laps")
    del laps[3]["lap_number"]
    with pytest.raises(ValueError, match=r"laps record #3 has no lap_number"):
        _rows("laps", (*KEY_COLUMNS["laps"], "meeting_key"), laps, "run")


def test_missing_meeting_key_fails_loudly() -> None:
    stints = records("stints")
    stints[0]["meeting_key"] = None
    with pytest.raises(ValueError, match="no meeting_key"):
        _rows("stints", (*KEY_COLUMNS["stints"], "meeting_key"), stints, "run")


def test_duplicate_key_fails_instead_of_merging() -> None:
    pit = records("pit")
    pit.append(copy.deepcopy(pit[0]))
    with pytest.raises(ValueError, match="more than one record"):
        _rows("pit", (*KEY_COLUMNS["pit"], "meeting_key"), pit, "run")


@pytest.mark.integration
def test_load_writes_every_record_with_payload_unchanged(conn: psycopg.Connection) -> None:
    written = load_all(PostgresSink(conn), "run-1")
    assert written == EXPECTED_COUNTS
    assert counts(conn) == EXPECTED_COUNTS
    payload = conn.execute("SELECT payload FROM raw.pit ORDER BY date LIMIT 1").fetchone()[0]  # type: ignore[index]
    assert payload == sorted(records("pit"), key=lambda r: r["date"])[0]
    row = conn.execute("SELECT session_key, meeting_key, _run_id FROM raw.sessions").fetchone()
    assert row == (9472, 1229, "run-1")


@pytest.mark.integration
def test_second_load_is_idempotent(conn: psycopg.Connection) -> None:
    sink = PostgresSink(conn)
    load_all(sink, "run-1")
    first_payloads = conn.execute("SELECT payload FROM raw.laps ORDER BY 1").fetchall()
    first_ts = conn.execute("SELECT min(_ingested_at) FROM raw.laps").fetchone()[0]  # type: ignore[index]

    load_all(sink, "run-2")

    assert counts(conn) == EXPECTED_COUNTS
    assert conn.execute("SELECT payload FROM raw.laps ORDER BY 1").fetchall() == first_payloads
    # The rows were rewritten, not duplicated: provenance moved to the second run.
    assert conn.execute("SELECT DISTINCT _run_id FROM raw.laps").fetchall() == [("run-2",)]
    assert conn.execute("SELECT min(_ingested_at) FROM raw.laps").fetchone()[0] > first_ts  # type: ignore[index]
    for endpoint in ENDPOINTS:
        keys = ", ".join(KEY_COLUMNS[endpoint])
        duplicates = conn.execute(
            f"SELECT count(*) FROM (SELECT 1 FROM raw.{endpoint} GROUP BY {keys} "
            "HAVING count(*) > 1) d"
        ).fetchone()
        assert duplicates == (0,)


@pytest.mark.integration
def test_corrected_record_overwrites_payload(conn: psycopg.Connection) -> None:
    sink = PostgresSink(conn)
    load_all(sink, "run-1")
    laps = records("laps")
    laps[0]["lap_duration"] = 1.234
    sink.upsert("laps", laps, "run-2")
    assert counts(conn)["laps"] == EXPECTED_COUNTS["laps"]
    stored = conn.execute(
        "SELECT payload->>'lap_duration' FROM raw.laps "
        "WHERE session_key = %s AND driver_number = %s AND lap_number = %s",
        (laps[0]["session_key"], laps[0]["driver_number"], laps[0]["lap_number"]),
    ).fetchone()
    assert stored == ("1.234",)


@pytest.mark.integration
def test_error_mid_endpoint_leaves_nothing_written(conn: psycopg.Connection) -> None:
    drivers = records("drivers")
    drivers[-1]["driver_number"] = "not a number"  # passes our check, rejected by the database
    with pytest.raises(psycopg.Error):
        PostgresSink(conn).upsert("drivers", drivers, "run-1")
    assert counts(conn)["drivers"] == 0
