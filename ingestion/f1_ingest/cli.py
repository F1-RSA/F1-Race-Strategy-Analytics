"""Command line entry point: `python -m f1_ingest.cli --season 2024 [--source jolpica]`."""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from datetime import UTC, datetime

import psycopg

from f1_ingest.config import Settings
from f1_ingest.http import HttpError
from f1_ingest.jolpica import JolpicaClient, PaginationError
from f1_ingest.jolpica_load import SeasonResult, run_season
from f1_ingest.openf1 import OpenF1Client, Record
from f1_ingest.pipeline import RunResult, not_ready_reason, run
from f1_ingest.sinks.postgres import connect

logger = logging.getLogger("f1_ingest")

EXIT_OK, EXIT_FAILED, EXIT_USAGE = 0, 1, 2

DEFAULT_SESSION_TYPES = ["Race"]  # OpenF1 files Sprint sessions under type "Race" as well


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m f1_ingest.cli",
        description="Load OpenF1 sessions (sessions, drivers, laps, stints, pit) or Jolpica "
        "races and results into raw.*. Database and API settings come from environment "
        "variables (see .env.example).",
        epilog="Exit code: 0 all ok, 1 at least one session or round failed, 2 invalid use.",
    )
    parser.add_argument(
        "--source",
        choices=["openf1", "jolpica"],
        default="openf1",
        help="openf1 (default); jolpica loads the race results of --season and takes "
        "only --season, --mode and --target",
    )
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--season", type=int, help="load the finished sessions of this year")
    selection.add_argument("--session-key", type=int, help="load this one session")
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="incremental",
        help="incremental (default) skips sessions that are already loaded; "
        "full reloads every selected session (safe: writes are upserts)",
    )
    parser.add_argument(
        "--target",
        choices=["local", "cloud"],
        default="local",
        help="local = PostgreSQL (default); cloud is not implemented yet",
    )
    parser.add_argument(
        "--session-type",
        action="append",
        dest="session_types",
        metavar="TYPE",
        help="OpenF1 session_type to load with --season, repeatable (default: Race, "
        "which includes Sprint)",
    )
    parser.add_argument(
        "--meeting-limit",
        type=int,
        metavar="N",
        help="with --season: only the first N finished race weekends of the season",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.meeting_limit is not None and args.meeting_limit < 1:
        parser.error("--meeting-limit must be at least 1")
    if args.session_key is not None and (args.meeting_limit or args.session_types):
        parser.error("--meeting-limit and --session-type only apply to --season")
    if args.source == "jolpica" and (
        args.season is None or args.meeting_limit or args.session_types
    ):
        parser.error("--source jolpica needs --season and takes no --session-type/--meeting-limit")
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    if args.target == "cloud":
        logger.error("--target cloud is not implemented yet (final milestone)")
        return EXIT_USAGE
    try:
        settings = Settings.from_env()
    except ValueError as exc:
        logger.error("invalid configuration: %s", exc)
        return EXIT_USAGE

    try:
        conn = connect(settings)
    except psycopg.OperationalError as exc:
        logger.error(
            "cannot connect to postgres at %s:%s: %s",
            settings.postgres_host,
            settings.postgres_port,
            exc,
        )
        return EXIT_USAGE
    try:
        if args.source == "jolpica":
            return _run_jolpica(args, settings, conn)
        return _run_openf1(args, settings, conn)
    finally:
        conn.close()


def _run_openf1(args: argparse.Namespace, settings: Settings, conn: psycopg.Connection) -> int:
    with OpenF1Client.from_settings(settings) as client:
        try:
            sessions = _list_sessions(client, args.season, args.session_key)
        except HttpError as exc:
            logger.error("could not list sessions: %s", exc)
            return EXIT_FAILED
        if not sessions:
            logger.error("OpenF1 returned no sessions for %s", args.season or args.session_key)
            return EXIT_FAILED
        if args.session_key is not None:
            # Named explicitly, so refuse loudly instead of skipping with only a log line.
            reason = not_ready_reason(sessions[0], datetime.now(UTC))
            if reason:
                logger.error("session %s is %s; nothing was loaded", args.session_key, reason)
                return EXIT_FAILED
        result = run(
            sessions,
            client=client,
            conn=conn,
            mode=args.mode,
            # An explicit key is loaded whatever its type: the user named it.
            session_types=None
            if args.session_key is not None
            else args.session_types or DEFAULT_SESSION_TYPES,
            meeting_limit=args.meeting_limit,
        )
    _log_summary(result)
    return EXIT_OK if result.ok else EXIT_FAILED


def _run_jolpica(args: argparse.Namespace, settings: Settings, conn: psycopg.Connection) -> int:
    with JolpicaClient.from_settings(settings) as client:
        try:
            season = run_season(args.season, client=client, conn=conn, mode=args.mode)
        except (HttpError, PaginationError, LookupError) as exc:
            logger.error("season %s was not loaded completely: %s", args.season, exc)
            return EXIT_FAILED
    _log_season_summary(season)
    return EXIT_OK if season.ok else EXIT_FAILED


def _list_sessions(
    client: OpenF1Client, season: int | None, session_key: int | None
) -> list[Record]:
    if session_key is not None:
        return client.fetch("sessions", session_key)
    assert season is not None
    return client.list_sessions(season)


def _log_summary(result: RunResult) -> None:
    logger.info(
        "run %s finished: %d loaded, %d already loaded, %d not ready, %d failed",
        result.run_id,
        len(result.loaded),
        len(result.already_loaded),
        len(result.not_ready),
        len(result.failed),
    )
    for key, reason in result.failed.items():
        logger.error("session %s failed: %s", key, reason)


def _log_season_summary(result: SeasonResult) -> None:
    logger.info(
        "run %s finished: season %d, %d races in schedule, %d rounds loaded, %d already loaded, "
        "%d without results yet, %d not started, %d failed",
        result.run_id,
        result.season,
        result.races,
        len(result.loaded),
        len(result.already_loaded),
        len(result.pending),
        len(result.upcoming),
        len(result.failed),
    )
    for number, reason in result.failed.items():
        logger.error("round %s failed: %s", number, reason)


if __name__ == "__main__":
    sys.exit(main())
