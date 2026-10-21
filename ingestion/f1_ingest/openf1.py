"""OpenF1 client: fetches raw records per session and endpoint, never per driver or lap."""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx

from f1_ingest.config import Settings
from f1_ingest.http import HttpClient, HttpError, TokenBucket

logger = logging.getLogger(__name__)

Record = dict[str, Any]

ENDPOINTS = ("sessions", "drivers", "laps", "stints", "pit")

# OpenF1 answers an empty result with 404 and this detail instead of an empty list.
_NO_RESULTS_DETAIL = "No results found."


class OpenF1Client:
    """Returns the API records unchanged: no renaming, no dropped fields.

    The raw payload is stored as JSONB later, which protects us against schema drift
    such as pit_duration being replaced by lane_duration/stop_duration.
    """

    def __init__(self, http: HttpClient) -> None:
        self._http = http

    @classmethod
    def from_settings(
        cls, settings: Settings, transport: httpx.BaseTransport | None = None
    ) -> OpenF1Client:
        limiter = TokenBucket(
            rate_per_second=settings.openf1_requests_per_minute / 60,
            capacity=settings.openf1_burst,
        )
        http = HttpClient(
            settings.openf1_base_url,
            limiter,
            timeout=settings.http_timeout_seconds,
            max_retries=settings.http_max_retries,
            backoff_base=settings.http_backoff_base_seconds,
            backoff_max=settings.http_backoff_max_seconds,
            transport=transport,
        )
        return cls(http)

    def fetch(self, endpoint: str, session_key: int) -> list[Record]:
        """All records of one endpoint for one session; [] if OpenF1 has none."""
        # Checked here because OpenF1 answers unknown endpoints with the same 404 as "no data".
        if endpoint not in ENDPOINTS:
            raise ValueError(f"unknown OpenF1 endpoint {endpoint!r}, expected one of {ENDPOINTS}")
        records = self._get(endpoint, {"session_key": session_key})
        logger.info("openf1 %s session_key=%s: %d records", endpoint, session_key, len(records))
        return records

    def list_sessions(self, year: int) -> list[Record]:
        """All sessions of a season (every type); [] if OpenF1 has none."""
        records = self._get("sessions", {"year": year})
        logger.info("openf1 sessions year=%s: %d records", year, len(records))
        return records

    def _get(self, endpoint: str, params: dict[str, int]) -> list[Record]:
        try:
            records = self._http.get_json(endpoint, params)
        except HttpError as exc:
            if exc.status_code == 404 and _is_no_results(exc.body):
                return []
            raise
        if not isinstance(records, list):
            raise ValueError(
                f"OpenF1 {endpoint} returned {type(records).__name__}, expected a list"
            )
        return records

    def fetch_session(self, session_key: int) -> dict[str, list[Record]]:
        """All five endpoints for one session, or an exception: never a partial session."""
        return {endpoint: self.fetch(endpoint, session_key) for endpoint in ENDPOINTS}

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> OpenF1Client:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def _is_no_results(body: str) -> bool:
    try:
        payload = json.loads(body)
    except ValueError:
        return False
    return isinstance(payload, dict) and payload.get("detail") == _NO_RESULTS_DETAIL
