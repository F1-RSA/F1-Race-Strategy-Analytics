"""Shared HTTP layer: client-side rate limiting, retries and backoff for all API clients."""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from f1_ingest.config import Settings

logger = logging.getLogger(__name__)


class TokenBucket:
    """Client-side rate limiter: allows `capacity` requests at once, then `rate_per_second`.

    Throttling ourselves keeps us below the API limit instead of reacting to 429s after
    the fact. In any window of t seconds at most `capacity + rate_per_second * t` requests
    pass. Not thread-safe and not shared between processes: one ingestion run at a time.
    """

    def __init__(
        self,
        rate_per_second: float,
        capacity: int,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be greater than 0")
        if capacity < 1:
            raise ValueError("capacity must be at least 1")
        self._rate = rate_per_second
        self._capacity = float(capacity)
        self._clock = clock
        self._sleep = sleep
        self._tokens = float(capacity)
        self._updated = clock()

    def acquire(self) -> float:
        """Take one token, sleeping until one is available. Returns the seconds waited."""
        self._refill()
        waited = 0.0
        if self._tokens < 1:
            waited = (1 - self._tokens) / self._rate
            self._sleep(waited)
            self._refill()
        # May go a hair below zero through float rounding; the next wait absorbs it.
        self._tokens -= 1
        return waited

    def _refill(self) -> None:
        now = self._clock()
        self._tokens = min(self._capacity, self._tokens + (now - self._updated) * self._rate)
        self._updated = now


class HttpError(Exception):
    """A request failed with a status that retrying cannot fix (4xx other than 429)."""

    def __init__(self, message: str, status_code: int | None = None, body: str = "") -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class RetriesExhaustedError(HttpError):
    """A request still failed with a retryable error after the last retry."""


class HttpClient:
    """GET JSON through the rate limiter, retrying 429, 5xx, timeouts and connection errors.

    Each attempt, retries included, takes a token from the limiter, so retries cannot
    exceed the configured rate either. Between retries it waits for the server's
    Retry-After if given, else a random delay in [0, min(backoff_max, backoff_base * 2^n)]
    for retry n (full jitter: parallel clients do not retry in lockstep).
    """

    def __init__(
        self,
        base_url: str,
        limiter: TokenBucket,
        *,
        timeout: float,
        max_retries: int,
        backoff_base: float,
        backoff_max: float,
        headers: Mapping[str, str] | None = None,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
        rng: random.Random | None = None,
    ) -> None:
        self._client = httpx.Client(
            base_url=base_url,
            timeout=timeout,
            transport=transport,
            headers={"Accept": "application/json", **(headers or {})},
        )
        self._limiter = limiter
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._backoff_max = backoff_max
        self._sleep = sleep
        self._rng = rng or random.Random()

    def get_json(self, path: str, params: Mapping[str, str | int] | None = None) -> Any:
        attempts = self._max_retries + 1
        for attempt in range(attempts):
            self._limiter.acquire()
            status_code: int | None = None
            retry_after: float | None = None
            try:
                response = self._client.get(path, params=params)
            except httpx.TransportError as exc:
                reason = f"{type(exc).__name__}: {exc}"
            else:
                status_code = response.status_code
                if response.is_success:
                    return response.json()
                if status_code != 429 and status_code < 500:
                    raise HttpError(
                        f"GET {response.url} returned {status_code}",
                        status_code=status_code,
                        body=response.text,
                    )
                reason = f"HTTP {status_code}"
                retry_after = parse_retry_after(
                    response.headers.get("Retry-After"), datetime.now(UTC)
                )

            if attempt == attempts - 1:
                raise RetriesExhaustedError(
                    f"GET {path} {dict(params or {})} failed after {attempts} attempts: {reason}",
                    status_code=status_code,
                )
            delay = retry_after if retry_after is not None else self._backoff(attempt)
            logger.warning(
                "GET %s %s: %s, retry %d/%d in %.1fs",
                path,
                dict(params or {}),
                reason,
                attempt + 1,
                self._max_retries,
                delay,
            )
            self._sleep(delay)
        raise AssertionError("unreachable")

    def _backoff(self, attempt: int) -> float:
        return self._rng.uniform(0, min(self._backoff_max, self._backoff_base * 2**attempt))

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> HttpClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def build_http_client(
    settings: Settings,
    *,
    base_url: str,
    requests_per_minute: float,
    burst: int,
    headers: Mapping[str, str] | None = None,
    transport: httpx.BaseTransport | None = None,
) -> HttpClient:
    """One client per API: its own base URL, rate limit and headers, shared retry settings."""
    limiter = TokenBucket(rate_per_second=requests_per_minute / 60, capacity=burst)
    return HttpClient(
        base_url,
        limiter,
        timeout=settings.http_timeout_seconds,
        max_retries=settings.http_max_retries,
        backoff_base=settings.http_backoff_base_seconds,
        backoff_max=settings.http_backoff_max_seconds,
        headers=headers,
        transport=transport,
    )


def parse_retry_after(value: str | None, now: datetime) -> float | None:
    """Seconds to wait from a Retry-After header (delta-seconds or HTTP-date), None if unusable."""
    if value is None:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        return None
    return max(0.0, (when - now).total_seconds())
