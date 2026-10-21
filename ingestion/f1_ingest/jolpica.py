"""Jolpica (Ergast-compatible) client: season schedule and race results, all pages merged."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from f1_ingest.config import Settings
from f1_ingest.http import HttpClient, build_http_client

logger = logging.getLogger(__name__)

Record = dict[str, Any]

# Jolpica caps `limit` at 100 and echoes the cap, so the page size we ask for is only a wish.
MAX_PAGE_SIZE = 100


class PaginationError(Exception):
    """The pages of one request do not add up; nothing from it may be loaded."""


class JolpicaClient:
    """Returns races as the API sends them: same fields, values still strings.

    Jolpica paginates over result ROWS, not races, so a race can start on one page and end on
    the next. The client puts such a race back together, so callers see each race once with
    all its results, as if the answer had come in a single page.
    """

    def __init__(self, http: HttpClient, page_size: int = MAX_PAGE_SIZE) -> None:
        if not 1 <= page_size <= MAX_PAGE_SIZE:
            raise ValueError(f"page_size must be between 1 and {MAX_PAGE_SIZE}")
        self._http = http
        self._page_size = page_size

    @classmethod
    def from_settings(
        cls,
        settings: Settings,
        transport: httpx.BaseTransport | None = None,
        page_size: int = MAX_PAGE_SIZE,
    ) -> JolpicaClient:
        http = build_http_client(
            settings,
            base_url=settings.jolpica_base_url,
            requests_per_minute=settings.jolpica_requests_per_minute,
            burst=settings.jolpica_burst,
            headers={"User-Agent": settings.jolpica_user_agent},
            transport=transport,
        )
        return cls(http, page_size)

    def fetch_races(self, season: int) -> list[Record]:
        """The season schedule; [] if the season is unknown."""
        return self._paginate(f"{season}/races.json")

    def fetch_results(self, season: int) -> list[Record]:
        """All race results of a season, each race once with its complete `Results` list."""
        return self._paginate(f"{season}/results.json")

    def _paginate(self, path: str) -> list[Record]:
        races: list[Record] = []
        offset = 0
        received = 0
        total: int | None = None
        while True:
            payload = self._http.get_json(path, {"limit": self._page_size, "offset": offset})
            try:
                mr = payload["MRData"]
                page_total, page_limit, page_offset = (
                    int(mr["total"]),
                    int(mr["limit"]),
                    int(mr["offset"]),
                )
                page_races = mr["RaceTable"]["Races"]
                rows = sum(_row_count(race) for race in page_races)
            except (KeyError, TypeError, ValueError) as exc:
                raise PaginationError(
                    f"GET {path}: unexpected response structure: {exc!r}"
                ) from exc

            if total is None:
                total = page_total
            elif page_total != total:
                raise PaginationError(
                    f"GET {path}: total changed from {total} to {page_total} at offset {offset}; "
                    "the data changed while paging, rerun"
                )
            if page_offset != offset:
                raise PaginationError(f"GET {path}: asked offset {offset}, got {page_offset}")
            if rows == 0 and received < total:
                raise PaginationError(f"GET {path}: empty page at offset {offset} of {total} rows")
            # The server's own limit counts, not ours: it may cap what we asked for.
            expected = min(page_limit, total - offset)
            if rows != expected:
                raise PaginationError(
                    f"GET {path}: offset {offset} returned {rows} rows, expected {expected} "
                    f"(limit {page_limit}, total {total})"
                )
            _merge(races, page_races, path)
            received += rows
            offset += rows
            if received >= total:
                logger.info("jolpica %s: %d rows, %d races", path, received, len(races))
                return races

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> JolpicaClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def _row_count(race: Record) -> int:
    # The schedule has one row per race, results have one row per result.
    return len(race["Results"]) if "Results" in race else 1


def _merge(races: list[Record], page_races: list[Record], path: str) -> None:
    for race in page_races:
        previous = races[-1] if races else None
        if previous is None or _key(previous) != _key(race):
            races.append(race)
            continue
        # Same race again: only legal as the continuation of a result list over a page border.
        if "Results" not in race or "Results" not in previous:
            raise PaginationError(f"GET {path}: round {race['round']} appears twice")
        if _envelope(previous) != _envelope(race):
            raise PaginationError(f"GET {path}: round {race['round']} differs between two pages")
        previous["Results"].extend(race["Results"])


def _key(race: Record) -> tuple[str, str]:
    return race["season"], race["round"]


def _envelope(race: Record) -> Record:
    return {name: value for name, value in race.items() if name != "Results"}
