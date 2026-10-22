import httpx
import psycopg
import pytest
from test_openf1 import FakeOpenF1
from test_pipeline import RACE

from f1_ingest import cli, transform
from f1_ingest.config import Settings
from f1_ingest.sinks.postgres import connect
from f1_ingest.transform import GrainCheck


def test_help_needs_no_arguments(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as info:
        transform.main(["--help"])
    assert info.value.code == 0
    assert "core.laps_enriched" in capsys.readouterr().out


def test_unexpected_arguments_exit_with_2() -> None:
    with pytest.raises(SystemExit) as info:
        transform.main(["--season", "2024"])
    assert info.value.code == 2


def test_unreachable_database_exits_with_2(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def refuse(settings: Settings) -> None:
        raise psycopg.OperationalError("connection refused")

    monkeypatch.setattr(transform, "connect", refuse)
    assert transform.main([]) == 2
    assert "cannot connect" in caplog.text


@pytest.fixture
def standalone(monkeypatch: pytest.MonkeyPatch, test_settings: Settings) -> None:
    monkeypatch.setattr(transform, "connect", lambda settings: connect(test_settings))


def load_one_session(monkeypatch: pytest.MonkeyPatch, test_settings: Settings) -> None:
    api = FakeOpenF1({"sessions": [httpx.Response(200, json=[RACE])]})
    monkeypatch.setattr(cli.OpenF1Client, "from_settings", lambda s: api.client(max_retries=2))
    monkeypatch.setattr(cli, "connect", lambda settings: connect(test_settings))
    assert cli.main(["--season", "2024", "--meeting-limit", "1"]) == 0


@pytest.mark.integration
def test_standalone_run_is_repeatable_and_changes_nothing(
    standalone, conn: psycopg.Connection, monkeypatch: pytest.MonkeyPatch, test_settings: Settings
) -> None:
    load_one_session(monkeypatch, test_settings)
    query = "SELECT count(*), count(compound), sum(tyre_age_laps) FROM core.laps_enriched"
    assert transform.main([]) == 0
    first = conn.execute(query).fetchone()
    assert first is not None and first[0] == 114
    assert transform.main([]) == 0
    assert conn.execute(query).fetchone() == first


@pytest.mark.integration
def test_standalone_run_on_an_empty_database_passes(standalone, conn: psycopg.Connection) -> None:
    assert transform.main([]) == 0
    assert conn.execute("SELECT count(*) FROM core.laps_enriched").fetchone() == (0,)


@pytest.mark.integration
def test_standalone_run_exits_1_when_the_grain_check_fails(
    standalone, conn: psycopg.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(transform, "check_grain", lambda c: GrainCheck(3, 120, 114))
    assert transform.main([]) == 1
