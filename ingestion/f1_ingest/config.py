"""Settings read from environment variables only (see .env.example)."""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class Settings:
    openf1_base_url: str = "https://api.openf1.org/v1"
    # OpenF1 does not document or signal its limit; 24/min stays below the 30/min we assume.
    openf1_requests_per_minute: float = 24.0
    openf1_burst: int = 2
    http_timeout_seconds: float = 30.0
    http_max_retries: int = 5
    http_backoff_base_seconds: float = 1.0
    http_backoff_max_seconds: float = 60.0
    # Host default suits runs from the host; inside the compose network .env sets POSTGRES_HOST=db.
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str = "f1"
    postgres_password: str = ""
    postgres_db: str = "f1_db"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        env = os.environ if env is None else env
        defaults = cls()
        settings = cls(
            openf1_base_url=_read(env, "OPENF1_BASE_URL", str, defaults.openf1_base_url),
            openf1_requests_per_minute=_read(
                env, "OPENF1_REQUESTS_PER_MINUTE", float, defaults.openf1_requests_per_minute
            ),
            openf1_burst=_read(env, "OPENF1_BURST", int, defaults.openf1_burst),
            http_timeout_seconds=_read(
                env, "HTTP_TIMEOUT_SECONDS", float, defaults.http_timeout_seconds
            ),
            http_max_retries=_read(env, "HTTP_MAX_RETRIES", int, defaults.http_max_retries),
            http_backoff_base_seconds=_read(
                env, "HTTP_BACKOFF_BASE_SECONDS", float, defaults.http_backoff_base_seconds
            ),
            http_backoff_max_seconds=_read(
                env, "HTTP_BACKOFF_MAX_SECONDS", float, defaults.http_backoff_max_seconds
            ),
            postgres_host=_read(env, "POSTGRES_HOST", str, defaults.postgres_host),
            postgres_port=_read(env, "POSTGRES_PORT", int, defaults.postgres_port),
            postgres_user=_read(env, "POSTGRES_USER", str, defaults.postgres_user),
            postgres_password=_read(env, "POSTGRES_PASSWORD", str, defaults.postgres_password),
            postgres_db=_read(env, "POSTGRES_DB", str, defaults.postgres_db),
        )
        settings._validate()
        return settings

    def _validate(self) -> None:
        if self.openf1_requests_per_minute <= 0:
            raise ValueError("OPENF1_REQUESTS_PER_MINUTE must be greater than 0")
        if self.openf1_burst < 1:
            raise ValueError("OPENF1_BURST must be at least 1")
        if self.http_timeout_seconds <= 0:
            raise ValueError("HTTP_TIMEOUT_SECONDS must be greater than 0")
        if self.http_max_retries < 0:
            raise ValueError("HTTP_MAX_RETRIES must not be negative")
        if self.http_backoff_base_seconds < 0 or self.http_backoff_max_seconds < 0:
            raise ValueError("HTTP_BACKOFF_*_SECONDS must not be negative")


def _read(env: Mapping[str, str], name: str, cast: Callable[[str], T], default: T) -> T:
    # An empty value counts as unset, so a copied .env with blank lines keeps the defaults.
    raw = env.get(name, "").strip()
    if not raw:
        return default
    try:
        return cast(raw)
    except ValueError as exc:
        raise ValueError(f"{name}={raw!r} is not a valid {cast.__name__}") from exc
