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


