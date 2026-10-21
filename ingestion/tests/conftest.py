import dataclasses
import socket
import uuid
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest

from f1_ingest.config import Settings

DB_INIT = Path(__file__).resolve().parents[2] / "db" / "init"


@pytest.fixture(autouse=True)
def no_network(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail any test that tries to open a real connection; tests must use mocked transports."""
    if request.node.get_closest_marker("integration"):
        return  # these talk to the local Postgres, never to an API

    def refuse(*args: object, **kwargs: object) -> None:
        raise RuntimeError("network access is disabled in tests")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture(scope="session")
def test_settings() -> Iterator[Settings]:
    """Settings for a throwaway database on the compose server, with the real init scripts applied.

    A separate database keeps f1_db untouched and lets the tests run the unchanged DDL from
    db/init. Skipped when the server is unreachable (offline run, compose not started).
    """
    base = Settings.from_env()
    name = f"f1_test_{uuid.uuid4().hex[:8]}"
    try:
        admin = psycopg.connect(
            host=base.postgres_host,
            port=base.postgres_port,
            user=base.postgres_user,
            password=base.postgres_password,
            dbname="postgres",
            autocommit=True,
        )
    except psycopg.OperationalError as exc:
        pytest.skip(f"Postgres not reachable ({exc}); run `docker compose up -d db`")
    settings = dataclasses.replace(base, postgres_db=name)
    admin.execute(f'CREATE DATABASE "{name}"')
    try:
        with psycopg.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            user=settings.postgres_user,
            password=settings.postgres_password,
            dbname=name,
            autocommit=True,
        ) as conn:
            for script in sorted(DB_INIT.glob("*.sql")):
                conn.execute(script.read_text())
        yield settings
    finally:
        admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.close()


@pytest.fixture
def conn(test_settings: Settings) -> Iterator[psycopg.Connection]:
    """Autocommit connection to the test database, emptied before each test."""
    from f1_ingest.sinks.postgres import connect

    with connect(test_settings) as connection:
        connection.execute(
            "TRUNCATE raw.meetings, raw.sessions, raw.drivers, raw.laps, raw.stints, raw.pit, "
            "raw.jolpica_races, raw.jolpica_results, meta.load_log RESTART IDENTITY"
        )
        yield connection
