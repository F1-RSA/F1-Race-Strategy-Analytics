"""Loads one Jolpica season: the schedule, then the results of every completed round."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import psycopg

from f1_ingest.jolpica import JolpicaClient, Record
from f1_ingest.pipeline import Mode, new_run_id
from f1_ingest.sinks.postgres import PostgresSink
from f1_ingest.state import LoadLog

logger = logging.getLogger(__name__)

SOURCE = "jolpica"
RACES_KEYS = ("season", "round")
RESULTS_KEYS = ("season", "round", "driver_id")


@dataclass
class SeasonResult:
    run_id: str
    season: int
    races: int = 0  # schedule rows written to raw.jolpica_races
    loaded: list[int] = field(default_factory=list)  # rounds whose results were written
    already_loaded: list[int] = field(default_factory=list)
    pending: list[int] = field(default_factory=list)  # completed, but no results published yet
    upcoming: list[int] = field(default_factory=list)  # not started yet
    failed: dict[int, str] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.failed


def race_start(race: Record) -> datetime:
    """Start of the race in UTC. Old races have no `time`: they count from the next day."""
    if race.get("time"):
        start = datetime.fromisoformat(f"{race['date']}T{race['time']}")
        return start if start.tzinfo else start.replace(tzinfo=UTC)
    return datetime.fromisoformat(race["date"]).replace(tzinfo=UTC) + timedelta(days=1)


def result_rows(season: int, race: Record) -> list[dict[str, object]]:
    """One row per result. The payload is the race object with `Results` cut down to that one
    result, so it keeps the shape the API sent (values stay strings)."""
    envelope = {name: value for name, value in race.items() if name != "Results"}
    return [
        {
            "season": season,
            "round": int(race["round"]),
            "driver_id": result.get("Driver", {}).get("driverId"),
            "payload": {**envelope, "Results": [result]},
        }
        for result in race["Results"]
    ]


def run_season(
    season: int,
    *,
    client: JolpicaClient,
    conn: psycopg.Connection,
    mode: Mode,
    run_id: str | None = None,
    now: datetime | None = None,
) -> SeasonResult:
    """Write the schedule, then the results of the completed rounds that are not loaded yet.

    Incremental = completed rounds minus those with a `success` row for `results:<season>:<round>`;
    full = all completed rounds. Without rounds to load, no results are requested at all. A round
    is written together with its log row in one transaction, so a failed round stays "not loaded"
    and the others still run. Fetch errors (HttpError, PaginationError) are raised to the caller.
    """
    result = SeasonResult(run_id or new_run_id(), season)
    log = LoadLog(conn, SOURCE)
    sink = PostgresSink(conn)
    now = now or datetime.now(UTC)

    abandoned = log.abandon_stale_running(result.run_id)
    if abandoned:
        logger.warning("marked %d unfinished log rows of earlier runs as failed", abandoned)

    schedule = _load_schedule(season, client, sink, log, conn, result)
    schedule.sort(key=lambda race: int(race["round"]))
    completed = [int(r["round"]) for r in schedule if race_start(r) <= now]
    result.upcoming = [int(r["round"]) for r in schedule if race_start(r) > now]
    loaded = log.succeeded_endpoints(f"results:{season}:")
    if mode == "incremental":
        result.already_loaded = [n for n in completed if f"results:{season}:{n}" in loaded]
    to_load = [n for n in completed if n not in result.already_loaded]
    logger.info(
        "run %s season %d: %d to load, %d already loaded, %d not started",
        result.run_id,
        season,
        len(to_load),
        len(result.already_loaded),
        len(result.upcoming),
    )
    if not to_load:
        return result

    # The log row for the whole season fetch: its failure is visible in meta.load_log, too.
    fetch_id = log.start(result.run_id, f"results:{season}")
    try:
        races = {int(race["round"]): race for race in client.fetch_results(season)}
    except Exception:
        log.fail(fetch_id)
        raise
    log.finish(fetch_id, sum(len(race["Results"]) for race in races.values()))

    for number in to_load:
        if number not in races:
            result.pending.append(number)
            logger.info("round %d is over but has no results yet; a later run loads it", number)
            continue
        log_id = log.start(result.run_id, f"results:{season}:{number}")
        try:
            with conn.transaction():
                rows = sink.upsert_rows(
                    "jolpica_results",
                    RESULTS_KEYS,
                    result_rows(season, races[number]),
                    result.run_id,
                )
                log.finish(log_id, rows)
        except Exception as exc:
            logger.exception("round %d failed", number)
            log.fail(log_id)
            result.failed[number] = f"{type(exc).__name__}: {exc}"
        else:
            result.loaded.append(number)
            logger.info("round %d loaded (%d results)", number, rows)
    return result


def _load_schedule(
    season: int,
    client: JolpicaClient,
    sink: PostgresSink,
    log: LoadLog,
    conn: psycopg.Connection,
    result: SeasonResult,
) -> list[Record]:
    log_id = log.start(result.run_id, f"races:{season}")
    try:
        races = client.fetch_races(season)
        if not races:
            raise LookupError(f"Jolpica has no races for season {season}")
        rows = [{"season": season, "round": int(race["round"]), "payload": race} for race in races]
        with conn.transaction():
            result.races = sink.upsert_rows("jolpica_races", RACES_KEYS, rows, result.run_id)
            log.finish(log_id, result.races)
    except Exception:
        log.fail(log_id)
        raise
    return races
