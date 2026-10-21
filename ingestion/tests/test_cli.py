import httpx
import psycopg
import pytest
from test_openf1 import FakeOpenF1
from test_pipeline import RACE, counts

from f1_ingest import cli
from f1_ingest.config import Settings
from f1_ingest.openf1 import ENDPOINTS
from f1_ingest.sinks.postgres import connect


def test_help_lists_every_option(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as info:
        cli.main(["--help"])
    assert info.value.code == 0
    out = capsys.readouterr().out
    for option in ("--season", "--session-key", "--mode", "--target", "--meeting-limit"):
        assert option in out


@pytest.mark.parametrize(
    "argv",
    [
        [],
        ["--season", "2024", "--session-key", "9472"],
        ["--season", "2024", "--mode", "weekly"],
        ["--season", "2024", "--meeting-limit", "0"],
        ["--session-key", "9472", "--meeting-limit", "1"],
        ["--session-key", "9472", "--session-type", "Race"],
    ],
)
def test_invalid_arguments_exit_with_2(argv: list[str]) -> None:
    with pytest.raises(SystemExit) as info:
        cli.main(argv)
    assert info.value.code == 2


def test_cloud_target_is_refused_before_any_connection(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def forbidden(settings: Settings) -> None:
        raise AssertionError("must not connect")

    monkeypatch.setattr(cli, "connect", forbidden)
    assert cli.main(["--season", "2024", "--target", "cloud"]) == 2
    assert "not implemented yet (final milestone)" in caplog.text


def test_unreachable_database_exits_with_2(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def refuse(settings: Settings) -> None:
        raise psycopg.OperationalError("connection refused")

    monkeypatch.setattr(cli, "connect", refuse)
    assert cli.main(["--season", "2024"]) == 2
    assert "cannot connect to postgres" in caplog.text


@pytest.fixture
def wired(monkeypatch: pytest.MonkeyPatch, test_settings: Settings, conn: psycopg.Connection):
    """Runs cli.main against the test database and a fake OpenF1; returns the fake API."""

    def install(api: FakeOpenF1) -> FakeOpenF1:
        monkeypatch.setattr(
            cli.OpenF1Client, "from_settings", lambda settings: api.client(max_retries=2)
        )
        monkeypatch.setattr(cli, "connect", lambda settings: connect(test_settings))
        return api

    return install


@pytest.mark.integration
def test_season_run_loads_and_exits_0(wired, conn: psycopg.Connection) -> None:
    wired(FakeOpenF1({"sessions": [httpx.Response(200, json=[RACE])]}))
    assert cli.main(["--season", "2024", "--meeting-limit", "1"]) == 0
    assert counts(conn)["laps"] == 114


@pytest.mark.integration
def test_failed_session_exits_with_1(wired, conn: psycopg.Connection) -> None:
    wired(
        FakeOpenF1(
            {
                "sessions": [httpx.Response(200, json=[RACE])],
                "laps": [httpx.Response(503)] * 3,
            }
        )
    )
    assert cli.main(["--season", "2024"]) == 1
    status = conn.execute("SELECT status FROM meta.load_log WHERE endpoint = 'laps'").fetchone()
    assert status == ("failed",)


@pytest.mark.integration
def test_session_key_loads_that_session_whatever_its_type(wired, conn: psycopg.Connection) -> None:
    wired(FakeOpenF1())
    assert cli.main(["--session-key", "9472", "--mode", "full"]) == 0
    assert set(counts(conn)) == set(ENDPOINTS)
    assert counts(conn)["sessions"] == 1


@pytest.mark.integration
def test_unknown_session_key_exits_with_1(wired, conn: psycopg.Connection) -> None:
    wired(FakeOpenF1())
    assert cli.main(["--session-key", "1"]) == 1
