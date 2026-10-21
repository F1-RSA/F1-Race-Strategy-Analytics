"""Transformation step: builds the views of the core schema from the raw tables."""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from importlib import resources

import psycopg
from psycopg import sql

from f1_ingest.config import Settings
from f1_ingest.sinks.postgres import connect

logger = logging.getLogger(__name__)

EXIT_OK, EXIT_FAILED, EXIT_USAGE = 0, 1, 2  # the same codes as f1_ingest.cli

LAPS_ENRICHED_SQL = "laps_enriched.sql"
LAPS_ENRICHED = "core.laps_enriched"

# Share of laps without a stint in one session above which the report warns. The 2024 data stays
# below 1 % per session; a missing /stints answer would show as nearly 100 %.
COVERAGE_WARN_SHARE = 0.02
UNKNOWN_COMPOUNDS = ("UNKNOWN", "TEST_UNKNOWN")


def apply_view(conn: psycopg.Connection) -> None:
    """(Re)create core.laps_enriched. Safe to repeat: a view holds no data and is never stale."""
    sql_text = (resources.files("f1_ingest") / "sql" / LAPS_ENRICHED_SQL).read_text(
        encoding="utf-8"
    )
    conn.execute(sql_text)
    logger.info("core.laps_enriched created or replaced")


@dataclass(frozen=True)
class GrainCheck:
    duplicate_keys: int  # lap keys that appear more than once
    rows: int  # rows of the checked relation
    raw_laps: int  # rows of raw.laps, which the relation must reproduce one to one

    @property
    def ok(self) -> bool:
        return self.duplicate_keys == 0 and self.rows == self.raw_laps


@dataclass(frozen=True)
class Coverage:
    session_key: int | None  # None = all sessions together
    laps: int
    without_stint: int
    without_known_compound: int  # no stint, or compound UNKNOWN / TEST_UNKNOWN
    without_tyre_age: int
    without_lap_time: int

    @property
    def share_without_stint(self) -> float:
        return self.without_stint / self.laps if self.laps else 0.0


def _relation(name: str) -> sql.Identifier:
    # Names are identifiers, never interpolated as raw text.
    return sql.Identifier(*name.split("."))


def check_grain(conn: psycopg.Connection, relation: str = LAPS_ENRICHED) -> GrainCheck:
    """One row per lap: no key twice, and as many rows as raw.laps (the join loses no lap).

    raw.laps has the primary key, so a violation can only come from the join; the overlapping
    stints of a pit stop are the case this guards.
    """
    query = sql.SQL(
        "SELECT (SELECT count(*) FROM (SELECT 1 FROM {rel} "
        "GROUP BY session_key, driver_number, lap_number HAVING count(*) > 1) AS d), "
        "(SELECT count(*) FROM {rel}), (SELECT count(*) FROM raw.laps)"
    ).format(rel=_relation(relation))
    row = conn.execute(query).fetchone()
    assert row is not None
    return GrainCheck(*row)


def coverage(conn: psycopg.Connection, relation: str = LAPS_ENRICHED) -> list[Coverage]:
    """Laps without stint, known compound, tyre age and lap time: per session, then in total."""
    query = sql.SQL(
        "SELECT session_key, count(*), "
        "count(*) FILTER (WHERE stint_number IS NULL), "
        "count(*) FILTER (WHERE compound IS NULL OR compound = ANY(%s)), "
        "count(*) FILTER (WHERE tyre_age_laps IS NULL), "
        "count(*) FILTER (WHERE NOT has_lap_time) "
        "FROM {rel} GROUP BY ROLLUP (session_key) ORDER BY session_key NULLS LAST"
    ).format(rel=_relation(relation))
    return [Coverage(*row) for row in conn.execute(query, (list(UNKNOWN_COMPOUNDS),)).fetchall()]


def report(grain: GrainCheck, rows: list[Coverage]) -> None:
    """Log both checks. Only the grain is an error; coverage is information (and a warning)."""
    if grain.ok:
        logger.info("grain check passed: %d laps, no duplicate key", grain.rows)
    else:
        logger.error(
            "grain check FAILED: %d duplicate lap keys, %d rows in the view but %d in raw.laps",
            grain.duplicate_keys,
            grain.rows,
            grain.raw_laps,
        )
    for c in rows:
        label = "total" if c.session_key is None else f"session {c.session_key}"
        logger.info(
            "coverage %s: %d laps, %d without stint (%.2f %%), %d without known compound, "
            "%d without tyre age, %d without lap time",
            label,
            c.laps,
            c.without_stint,
            100 * c.share_without_stint,
            c.without_known_compound,
            c.without_tyre_age,
            c.without_lap_time,
        )
        if c.session_key is not None and c.share_without_stint > COVERAGE_WARN_SHARE:
            logger.warning(
                "session %s: %.1f %% of the laps have no stint (limit %.0f %%); "
                "check the stints of that session",
                c.session_key,
                100 * c.share_without_stint,
                100 * COVERAGE_WARN_SHARE,
            )


def run_transform(conn: psycopg.Connection) -> bool:
    """Create the views, then run the checks; False if the grain check failed.

    Repeatable at any time: the views hold no data and the checks only read.
    """
    apply_view(conn)
    grain = check_grain(conn)
    report(grain, coverage(conn))
    return grain.ok


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m f1_ingest.transform",
        description="Create core.laps_enriched from the raw tables and check it. Needs no "
        "arguments and can be repeated; database settings come from environment variables.",
        epilog="Exit code: 0 checks passed, 1 grain check failed, 2 configuration or database "
        "not usable.",
    )
    parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    try:
        conn = connect(Settings.from_env())
    except (ValueError, psycopg.OperationalError) as exc:
        logger.error("cannot connect to the database: %s", exc)
        return EXIT_USAGE
    try:
        return EXIT_OK if run_transform(conn) else EXIT_FAILED
    except psycopg.Error as exc:
        logger.error("transform failed: %s", exc)
        return EXIT_FAILED
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
