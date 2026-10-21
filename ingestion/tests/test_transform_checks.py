import logging

import psycopg
import pytest
from psycopg.types.json import Jsonb
from test_laps_enriched import SESSION, add_lap, add_stint

from f1_ingest.transform import (
    COVERAGE_WARN_SHARE,
    GrainCheck,
    apply_view,
    check_grain,
    coverage,
    report,
)


def seed_overlap(conn: psycopg.Connection) -> None:
    """Driver 1: laps 1 to 6, stint 1 ends on lap 3 and stint 2 starts on lap 3."""
    for lap in range(1, 7):
        add_lap(conn, 1, lap)
    add_stint(conn, 1, 1, 1, 3)
    add_stint(conn, 1, 2, 3, 6, compound="HARD")


@pytest.fixture
def view(conn: psycopg.Connection) -> psycopg.Connection:
    apply_view(conn)
    return conn


def test_grain_check_ok_property() -> None:
    assert GrainCheck(0, 10, 10).ok
    assert not GrainCheck(1, 10, 10).ok
    assert not GrainCheck(0, 9, 10).ok  # a lap got lost


@pytest.mark.integration
def test_grain_passes_with_overlapping_stints(view: psycopg.Connection) -> None:
    seed_overlap(view)
    assert check_grain(view) == GrainCheck(duplicate_keys=0, rows=6, raw_laps=6)


@pytest.mark.integration
def test_grain_passes_on_an_empty_database(view: psycopg.Connection) -> None:
    assert check_grain(view) == GrainCheck(0, 0, 0)


@pytest.mark.integration
def test_grain_fails_for_a_plain_join_that_duplicates_the_overlap_lap(
    view: psycopg.Connection,
) -> None:
    seed_overlap(view)
    view.execute(
        "CREATE VIEW core.laps_naive AS SELECT l.session_key, l.driver_number, l.lap_number "
        "FROM raw.laps l LEFT JOIN raw.stints s ON s.session_key = l.session_key "
        "AND s.driver_number = l.driver_number AND l.lap_number "
        "BETWEEN (s.payload->>'lap_start')::int AND (s.payload->>'lap_end')::int"
    )
    try:
        result = check_grain(view, "core.laps_naive")
    finally:
        view.execute("DROP VIEW core.laps_naive")
    assert result == GrainCheck(duplicate_keys=1, rows=7, raw_laps=6)
    assert not result.ok


@pytest.mark.integration
def test_grain_fails_when_the_view_loses_a_lap(view: psycopg.Connection) -> None:
    seed_overlap(view)
    view.execute(
        "CREATE VIEW core.laps_short AS SELECT * FROM core.laps_enriched WHERE lap_number <> 4"
    )
    try:
        result = check_grain(view, "core.laps_short")
    finally:
        view.execute("DROP VIEW core.laps_short")
    assert result == GrainCheck(duplicate_keys=0, rows=5, raw_laps=6)
    assert not result.ok


@pytest.mark.integration
def test_relation_name_is_quoted_not_interpolated(view: psycopg.Connection) -> None:
    with pytest.raises(psycopg.Error):  # the whole text is one (invalid) name
        check_grain(view, "core.laps_enriched; DROP TABLE raw.laps")
    assert view.execute("SELECT count(*) FROM raw.laps").fetchone() == (0,)


def seed_coverage(conn: psycopg.Connection) -> None:
    # Session 1, driver 1: 10 laps. Laps 9 and 10 have no stint, lap 2 has no lap time,
    # stint 2 (laps 6 to 8) has an UNKNOWN compound and no tyre age.
    for lap in range(1, 11):
        add_lap(conn, 1, lap, duration=None if lap == 2 else 90.0)
    add_stint(conn, 1, 1, 1, 5)
    add_stint(conn, 1, 2, 6, 8, compound="UNKNOWN", age=None)
    # Session 2: a single lap that is covered.
    conn.execute(
        "INSERT INTO raw.laps VALUES (2, 1, 1, 11, %s)",
        (Jsonb({"lap_duration": 80, "is_pit_out_lap": False}),),
    )
    conn.execute(
        "INSERT INTO raw.stints (session_key, driver_number, stint_number, meeting_key, payload) "
        "VALUES (2, 1, 1, 11, %s)",
        (Jsonb({"lap_start": 1, "lap_end": 5, "compound": "SOFT", "tyre_age_at_start": 0}),),
    )


@pytest.mark.integration
def test_coverage_is_reported_per_session_and_in_total(view: psycopg.Connection) -> None:
    seed_coverage(view)
    one, two, total = coverage(view)
    assert (one.session_key, one.laps, one.without_stint) == (SESSION, 10, 2)
    assert one.without_known_compound == 2 + 3  # no stint, plus UNKNOWN
    assert one.without_tyre_age == 2 + 3  # no stint, plus stint without tyre age
    assert one.without_lap_time == 1
    assert (two.session_key, two.laps, two.without_stint) == (2, 1, 0)
    assert (total.session_key, total.laps, total.without_stint) == (None, 11, 2)
    assert total.without_known_compound == 5


@pytest.mark.integration
def test_coverage_of_an_empty_database_is_empty(view: psycopg.Connection) -> None:
    assert [c.laps for c in coverage(view)] == [0]  # only the total row, no division by zero


@pytest.mark.integration
def test_coverage_warns_above_the_limit_but_is_no_failure(
    view: psycopg.Connection, caplog: pytest.LogCaptureFixture
) -> None:
    seed_coverage(view)  # session 1: 20 % without stint
    caplog.set_level(logging.INFO)
    report(check_grain(view), coverage(view))
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1 and "session 1" in warnings[0].getMessage()
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert "grain check passed" in caplog.text
    assert 0.2 > COVERAGE_WARN_SHARE


@pytest.mark.integration
def test_report_logs_an_error_for_a_failed_grain(
    view: psycopg.Connection, caplog: pytest.LogCaptureFixture
) -> None:
    report(GrainCheck(2, 12, 10), [])
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1 and "grain check FAILED" in errors[0].getMessage()
