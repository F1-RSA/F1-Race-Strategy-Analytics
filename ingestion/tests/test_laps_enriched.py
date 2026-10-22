from typing import Any

import psycopg
import pytest
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from f1_ingest.transform import apply_view

SESSION = 1


def add_lap(
    conn: psycopg.Connection,
    driver: int,
    lap: int,
    duration: float | None = 90.0,
    pit_out: bool = False,
) -> None:
    payload = {"lap_duration": duration, "is_pit_out_lap": pit_out, "lap_number": lap}
    conn.execute(
        "INSERT INTO raw.laps (session_key, driver_number, lap_number, meeting_key, payload) "
        "VALUES (%s, %s, %s, 10, %s)",
        (SESSION, driver, lap, Jsonb(payload)),
    )


def add_stint(
    conn: psycopg.Connection,
    driver: int,
    number: int,
    start: int | None,
    end: int | None,
    compound: str | None = "SOFT",
    age: int | None = 0,
) -> None:
    payload = {
        "lap_start": start,
        "lap_end": end,
        "compound": compound,
        "tyre_age_at_start": age,
        "stint_number": number,
    }
    conn.execute(
        "INSERT INTO raw.stints (session_key, driver_number, stint_number, meeting_key, payload) "
        "VALUES (%s, %s, %s, 10, %s)",
        (SESSION, driver, number, Jsonb(payload)),
    )


def rows(conn: psycopg.Connection) -> dict[tuple[int, int], dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM core.laps_enriched")
        return {(r["driver_number"], r["lap_number"]): r for r in cur.fetchall()}


@pytest.fixture
def view(conn: psycopg.Connection) -> psycopg.Connection:
    apply_view(conn)
    return conn


@pytest.mark.integration
def test_lap_inside_a_stint_gets_compound_stint_and_tyre_age(view: psycopg.Connection) -> None:
    add_lap(view, 1, 5)
    add_stint(view, 1, 1, start=1, end=10, compound="MEDIUM", age=3)
    row = rows(view)[(1, 5)]
    assert row["compound"] == "MEDIUM"
    assert row["stint_number"] == 1
    assert row["tyre_age_laps"] == 3 + (5 - 1)  # age at the start plus laps driven since


@pytest.mark.integration
def test_columns_and_types(view: psycopg.Connection) -> None:
    add_lap(view, 1, 1, duration=91.234)
    add_stint(view, 1, 1, 1, 5)
    row = rows(view)[(1, 1)]
    assert list(row) == [
        "session_key",
        "driver_number",
        "lap_number",
        "meeting_key",
        "lap_duration_s",
        "compound",
        "stint_number",
        "tyre_age_laps",
        "is_pit_out_lap",
        "has_lap_time",
    ]
    assert str(row["lap_duration_s"]) == "91.234"
    assert (row["session_key"], row["meeting_key"]) == (SESSION, 10)


@pytest.mark.integration
def test_lap_in_two_overlapping_stints_appears_once_in_the_earlier_one(
    view: psycopg.Connection,
) -> None:
    # As OpenF1 sends a pit stop in 9488: stint 1 ends on lap 4, stint 2 starts on lap 4.
    for lap in range(1, 9):
        add_lap(view, 1, lap)
    add_stint(view, 1, 1, 1, 4, compound="SOFT", age=2)
    add_stint(view, 1, 2, 4, 8, compound="HARD", age=0)
    result = rows(view)
    assert len(result) == 8
    assert (result[(1, 4)]["compound"], result[(1, 4)]["stint_number"]) == ("SOFT", 1)
    assert result[(1, 4)]["tyre_age_laps"] == 2 + 3
    assert (result[(1, 5)]["compound"], result[(1, 5)]["tyre_age_laps"]) == ("HARD", 1)


@pytest.mark.integration
def test_a_plain_between_join_would_have_returned_the_overlap_lap_twice(
    view: psycopg.Connection,
) -> None:
    for lap in range(1, 9):
        add_lap(view, 1, lap)
    add_stint(view, 1, 1, 1, 4)
    add_stint(view, 1, 2, 4, 8)
    naive = view.execute(
        "SELECT count(*) FROM raw.laps l JOIN raw.stints s ON s.session_key = l.session_key "
        "AND s.driver_number = l.driver_number AND l.lap_number "
        "BETWEEN (s.payload->>'lap_start')::int AND (s.payload->>'lap_end')::int"
    ).fetchone()
    assert naive == (9,)
    assert view.execute("SELECT count(*) FROM core.laps_enriched").fetchone() == (8,)


@pytest.mark.integration
def test_stints_of_other_drivers_and_sessions_are_not_used(view: psycopg.Connection) -> None:
    add_lap(view, 1, 3)
    add_stint(view, 2, 1, 1, 10, compound="HARD")  # another driver
    view.execute(
        "INSERT INTO raw.stints (session_key, driver_number, stint_number, meeting_key, payload) "
        "VALUES (2, 1, 1, 10, %s)",
        (Jsonb({"lap_start": 1, "lap_end": 10, "compound": "WET", "tyre_age_at_start": 0}),),
    )
    row = rows(view)[(1, 3)]
    assert row["compound"] is None and row["stint_number"] is None


@pytest.mark.integration
def test_lap_without_stint_is_kept_with_null_compound_and_tyre_age(
    view: psycopg.Connection,
) -> None:
    add_lap(view, 1, 1)
    add_lap(view, 1, 2)
    add_stint(view, 1, 1, 1, 1)
    row = rows(view)[(1, 2)]  # a retirement lap after the last stint
    assert row["compound"] is None
    assert row["stint_number"] is None
    assert row["tyre_age_laps"] is None
    assert row["has_lap_time"] is True


@pytest.mark.integration
def test_stint_without_lap_range_matches_no_lap(view: psycopg.Connection) -> None:
    add_lap(view, 1, 1)
    add_stint(view, 1, 1, start=None, end=None, compound="SOFT")
    row = rows(view)[(1, 1)]
    assert row["compound"] is None and row["tyre_age_laps"] is None


@pytest.mark.integration
def test_lap_without_duration_has_no_lap_time(view: psycopg.Connection) -> None:
    add_lap(view, 1, 1, duration=None)
    add_lap(view, 1, 2, duration=88.5)
    add_stint(view, 1, 1, 1, 5)
    result = rows(view)
    assert result[(1, 1)]["lap_duration_s"] is None
    assert result[(1, 1)]["has_lap_time"] is False
    assert result[(1, 2)]["has_lap_time"] is True


@pytest.mark.integration
def test_pit_out_lap_flag_is_carried_over(view: psycopg.Connection) -> None:
    add_lap(view, 1, 1)
    add_lap(view, 1, 2, pit_out=True)
    add_stint(view, 1, 1, 1, 1)
    add_stint(view, 1, 2, 2, 5, compound="HARD")
    result = rows(view)
    assert result[(1, 1)]["is_pit_out_lap"] is False
    assert result[(1, 2)]["is_pit_out_lap"] is True
    assert result[(1, 2)]["compound"] == "HARD"


@pytest.mark.integration
def test_missing_tyre_age_gives_no_tyre_age_laps_but_keeps_the_compound(
    view: psycopg.Connection,
) -> None:
    add_lap(view, 1, 3)
    add_stint(view, 1, 1, 1, 5, compound="INTERMEDIATE", age=None)
    row = rows(view)[(1, 3)]
    assert row["compound"] == "INTERMEDIATE"
    assert row["tyre_age_laps"] is None


@pytest.mark.integration
@pytest.mark.parametrize("compound", ["UNKNOWN", "TEST_UNKNOWN", None])
def test_unknown_and_missing_compounds_are_kept_as_the_api_sends_them(
    view: psycopg.Connection, compound: str | None
) -> None:
    add_lap(view, 1, 1)
    add_stint(view, 1, 1, 1, 5, compound=compound)
    row = rows(view)[(1, 1)]
    assert row["compound"] == compound
    assert row["stint_number"] == 1  # the lap has a stint, only its compound is unknown


@pytest.mark.integration
def test_one_row_per_lap_and_applying_twice_changes_nothing(view: psycopg.Connection) -> None:
    for driver in (1, 2):
        for lap in range(1, 6):
            add_lap(view, driver, lap)
        add_stint(view, driver, 1, 1, 3)
        add_stint(view, driver, 2, 3, 5, compound="HARD")
    first = rows(view)
    apply_view(view)
    assert rows(view) == first
    assert view.execute("SELECT count(*) FROM core.laps_enriched").fetchone() == (10,)
    assert view.execute("SELECT count(*) FROM raw.laps").fetchone() == (10,)
