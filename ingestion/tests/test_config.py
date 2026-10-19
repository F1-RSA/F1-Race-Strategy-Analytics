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
            env[key] = value
    assert Settings.from_env(env) == Settings()
