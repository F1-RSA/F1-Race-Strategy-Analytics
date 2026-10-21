from pathlib import Path

import pytest

from f1_ingest.config import Settings

ENV_EXAMPLE = Path(__file__).resolve().parents[2] / ".env.example"


def test_defaults_when_env_is_empty() -> None:
    assert Settings.from_env({}) == Settings()


def test_blank_values_fall_back_to_defaults() -> None:
    assert Settings.from_env({"OPENF1_BURST": "", "HTTP_MAX_RETRIES": "  "}) == Settings()


def test_values_are_read_and_cast() -> None:
    settings = Settings.from_env(
        {
            "OPENF1_BASE_URL": "http://localhost:8000/v1",
            "OPENF1_REQUESTS_PER_MINUTE": "12.5",
            "OPENF1_BURST": "3",
            "HTTP_TIMEOUT_SECONDS": "5",
            "HTTP_MAX_RETRIES": "0",
            "HTTP_BACKOFF_BASE_SECONDS": "0.5",
            "HTTP_BACKOFF_MAX_SECONDS": "10",
        }
    )
    assert settings == Settings(
        openf1_base_url="http://localhost:8000/v1",
        openf1_requests_per_minute=12.5,
        openf1_burst=3,
        http_timeout_seconds=5.0,
        http_max_retries=0,
        http_backoff_base_seconds=0.5,
        http_backoff_max_seconds=10.0,
    )


def test_invalid_number_names_the_variable() -> None:
    with pytest.raises(ValueError, match="OPENF1_BURST"):
        Settings.from_env({"OPENF1_BURST": "two"})


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("OPENF1_REQUESTS_PER_MINUTE", "0"),
        ("OPENF1_BURST", "0"),
        ("HTTP_TIMEOUT_SECONDS", "0"),
        ("HTTP_MAX_RETRIES", "-1"),
        ("HTTP_BACKOFF_BASE_SECONDS", "-1"),
    ],
)
def test_out_of_range_values_are_rejected(name: str, value: str) -> None:
    with pytest.raises(ValueError, match=name.split("_")[0]):
        Settings.from_env({name: value})


def test_env_example_matches_defaults() -> None:
    """`cp .env.example .env` must give the same settings as the code defaults."""
    env = {}
    for line in ENV_EXAMPLE.read_text().splitlines():
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            # The database values belong to compose (POSTGRES_HOST=db only resolves in its
            # network); the code defaults target a run from the host instead.
            if not key.startswith("POSTGRES_"):
                env[key] = value
    assert Settings.from_env(env) == Settings()


def test_postgres_settings_come_from_env() -> None:
    settings = Settings.from_env(
        {"POSTGRES_HOST": "db", "POSTGRES_PORT": "5433", "POSTGRES_PASSWORD": "secret"}
    )
    assert (settings.postgres_host, settings.postgres_port) == ("db", 5433)
    assert (settings.postgres_user, settings.postgres_db) == ("f1", "f1_db")
    assert settings.postgres_password == "secret"


def test_jolpica_settings_come_from_env() -> None:
    settings = Settings.from_env(
        {"JOLPICA_REQUESTS_PER_MINUTE": "3", "JOLPICA_BURST": "1", "JOLPICA_USER_AGENT": "me/1"}
    )
    assert (settings.jolpica_requests_per_minute, settings.jolpica_burst) == (3.0, 1)
    assert settings.jolpica_user_agent == "me/1"
    assert settings.jolpica_base_url == "https://api.jolpi.ca/ergast/f1"


def test_jolpica_default_rate_stays_below_the_documented_limits() -> None:
    settings = Settings()
    assert settings.jolpica_requests_per_minute * 60 < 500  # sustained limit per hour
    assert settings.jolpica_burst < 4  # burst limit per second


@pytest.mark.parametrize("env", [{"JOLPICA_REQUESTS_PER_MINUTE": "0"}, {"JOLPICA_BURST": "0"}])
def test_invalid_jolpica_rate_is_rejected(env: dict[str, str]) -> None:
    with pytest.raises(ValueError, match="JOLPICA"):
        Settings.from_env(env)
