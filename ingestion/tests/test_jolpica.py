import copy
import json
import random
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest

from f1_ingest.config import Settings
from f1_ingest.http import HttpClient, RetriesExhaustedError, TokenBucket
from f1_ingest.jolpica import JolpicaClient, PaginationError

FIXTURES = Path(__file__).parent / "fixtures" / "jolpica"
BASE_URL = "https://api.jolpi.ca/ergast/f1"
SERVER_MAX_LIMIT = 100


def load(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text())["MRData"]


ALL_RACES: list[dict[str, Any]] = load("races_2024.json")["RaceTable"]["Races"]
# The races exactly as the API sends them when everything fits on one page.
RESULT_RACES: list[dict[str, Any]] = [
    load(f"results_2024_round_{n}.json")["RaceTable"]["Races"][0] for n in (1, 2, 3)
]
ROWS = [
    ({k: v for k, v in race.items() if k != "Results"}, result)
    for race in RESULT_RACES
    for result in race["Results"]
]

Tamper = Callable[[dict[str, Any]], dict[str, Any] | httpx.Response]


def mr_data(limit: int, offset: int, total: int, races: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "MRData": {
            "xmlns": "",
            "series": "f1",
            "limit": str(limit),
            "offset": str(offset),
            "total": str(total),
            "RaceTable": {"season": "2024", "Races": races},
        }
    }


class FakeJolpica:
    """Answers like the live API: limit capped, offset and total counted over rows.

    `tamper` maps a request number (0-based) to a function that changes that answer or
    replaces it by a raw response, to simulate a misbehaving server.
    """

    def __init__(self, cap: int = SERVER_MAX_LIMIT, tamper: dict[int, Tamper] | None = None):
        self.cap = cap
        self.tamper = tamper or {}
        self.requests: list[httpx.Request] = []
        self.sleeps: list[float] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        limit = min(int(request.url.params["limit"]), self.cap)
        offset = int(request.url.params["offset"])
        payload = self._answer(request.url.path, limit, offset)
        step = self.tamper.get(len(self.requests) - 1)
        if step is not None:
            payload = step(payload)
            if isinstance(payload, httpx.Response):
                return payload
        return httpx.Response(200, json=payload)

    def _answer(self, path: str, limit: int, offset: int) -> dict[str, Any]:
        if path.endswith("/2024/races.json"):
            races = copy.deepcopy(ALL_RACES[offset : offset + limit])
            return mr_data(limit, offset, len(ALL_RACES), races)
        if not path.endswith("/2024/results.json"):
            return mr_data(limit, offset, 0, [])  # unknown season: 200, total 0
        races: list[dict[str, Any]] = []
        for envelope, result in ROWS[offset : offset + limit]:
            if races and races[-1]["round"] == envelope["round"]:
                races[-1]["Results"].append(copy.deepcopy(result))
            else:
                races.append({**copy.deepcopy(envelope), "Results": [copy.deepcopy(result)]})
        return mr_data(limit, offset, len(ROWS), races)

    def client(self, page_size: int = 100, max_retries: int = 2) -> JolpicaClient:
        clock = [0.0]

        def sleep(seconds: float) -> None:
            self.sleeps.append(seconds)
            clock[0] += seconds

        http = HttpClient(
            BASE_URL,
            TokenBucket(0.1, 2, clock=lambda: clock[0], sleep=sleep),  # 6 per minute, burst 2
            timeout=5,
            max_retries=max_retries,
            backoff_base=1,
            backoff_max=60,
            transport=httpx.MockTransport(self.handler),
            sleep=sleep,
            rng=random.Random(0),
        )
        return JolpicaClient(http, page_size)

    def offsets(self) -> list[int]:
        return [int(r.url.params["offset"]) for r in self.requests]


def rows_per_race(races: list[dict[str, Any]]) -> list[int]:
    return [len(race["Results"]) for race in races]


def test_fixtures_are_the_expected_building_blocks() -> None:
    assert rows_per_race(RESULT_RACES) == [20, 20, 19]
    assert len(ALL_RACES) == 24


def test_single_page_returns_races_unchanged() -> None:
    api = FakeJolpica()
    with api.client() as client:
        races = client.fetch_results(2024)
    assert races == RESULT_RACES
    assert api.offsets() == [0]
    assert str(api.requests[0].url) == f"{BASE_URL}/2024/results.json?limit=100&offset=0"


@pytest.mark.parametrize("page_size", [25, 7, 20, 1])
def test_page_border_inside_a_race_loses_and_duplicates_nothing(page_size: int) -> None:
    api = FakeJolpica()
    with api.client(page_size) as client:
        races = client.fetch_results(2024)
    assert races == RESULT_RACES
    assert rows_per_race(races) == [20, 20, 19]
    assert len(api.requests) == -(-len(ROWS) // page_size)  # ceil: one request per page


def test_page_size_25_puts_the_first_border_inside_round_2() -> None:
    api = FakeJolpica()
    with api.client(25) as client:
        client.fetch_results(2024)
    assert api.offsets() == [0, 25, 50]
    first_page = api._answer("/ergast/f1/2024/results.json", 25, 0)["MRData"]["RaceTable"]["Races"]
    assert [(r["round"], len(r["Results"])) for r in first_page] == [("1", 20), ("2", 5)]


def test_races_are_paginated_too() -> None:
    api = FakeJolpica()
    with api.client(10) as client:
        assert client.fetch_races(2024) == ALL_RACES
    assert api.offsets() == [0, 10, 20]


def test_empty_season_returns_no_races_after_one_request() -> None:
    api = FakeJolpica()
    with api.client() as client:
        assert client.fetch_results(1900) == []
        assert client.fetch_races(1900) == []
    assert api.offsets() == [0, 0]


def test_server_limit_below_the_requested_one_is_followed() -> None:
    api = FakeJolpica(cap=10)
    with api.client(25) as client:
        races = client.fetch_results(2024)
    assert races == RESULT_RACES
    assert api.offsets() == [0, 10, 20, 30, 40, 50]  # advanced by the rows received


def test_pagination_stays_within_the_rate_limit() -> None:
    api = FakeJolpica()
    with api.client(25) as client:
        client.fetch_results(2024)
    # Burst of 2, then one request every 10 s at 6 per minute.
    assert api.sleeps == [pytest.approx(10.0)]


def test_429_with_retry_after_is_retried_and_the_page_is_not_skipped() -> None:
    api = FakeJolpica(tamper={1: lambda _: httpx.Response(429, headers={"Retry-After": "7"})})
    with api.client(25) as client:
        races = client.fetch_results(2024)
    assert races == RESULT_RACES
    assert 7.0 in api.sleeps
    assert api.offsets() == [0, 25, 25, 50]  # the throttled page was asked for again


def test_exhausted_retries_fail_the_fetch() -> None:
    api = FakeJolpica(tamper={n: lambda _: httpx.Response(503) for n in (1, 2, 3)})
    with api.client(25, max_retries=2) as client, pytest.raises(RetriesExhaustedError):
        client.fetch_results(2024)


def set_total(value: int) -> Tamper:
    def tamper(payload: dict[str, Any]) -> dict[str, Any]:
        payload["MRData"]["total"] = str(value)
        return payload

    return tamper


def drop_last_result(payload: dict[str, Any]) -> dict[str, Any]:
    payload["MRData"]["RaceTable"]["Races"][-1]["Results"].pop()
    return payload


def empty_page(payload: dict[str, Any]) -> dict[str, Any]:
    payload["MRData"]["RaceTable"]["Races"] = []
    return payload


def rename_race(payload: dict[str, Any]) -> dict[str, Any]:
    payload["MRData"]["RaceTable"]["Races"][0]["raceName"] = "Another Grand Prix"
    return payload


def wrong_offset(payload: dict[str, Any]) -> dict[str, Any]:
    payload["MRData"]["offset"] = "0"
    return payload


def not_the_api(payload: dict[str, Any]) -> dict[str, Any]:
    return {"detail": "maintenance"}


@pytest.mark.parametrize(
    ("tamper", "message"),
    [
        (set_total(60), "total changed from 59 to 60"),
        (drop_last_result, "returned 24 rows, expected 25"),
        (empty_page, "empty page at offset 25"),
        (rename_race, "differs between two pages"),
        (wrong_offset, "asked offset 25, got 0"),
        (not_the_api, "unexpected response structure"),
    ],
)
def test_inconsistent_pages_fail_loudly(tamper: Tamper, message: str) -> None:
    api = FakeJolpica(tamper={1: tamper})
    with api.client(25) as client, pytest.raises(PaginationError, match=message):
        client.fetch_results(2024)


def test_more_rows_than_total_fail() -> None:
    def extra_row(payload: dict[str, Any]) -> dict[str, Any]:
        race = payload["MRData"]["RaceTable"]["Races"][0]
        race["Results"].append(copy.deepcopy(race["Results"][0]))
        return payload

    api = FakeJolpica(tamper={0: extra_row})
    with api.client(25) as client, pytest.raises(PaginationError, match="expected 25"):
        client.fetch_results(2024)


def test_total_changing_on_the_last_page_is_detected() -> None:
    api = FakeJolpica(tamper={2: set_total(100)})
    with api.client(25) as client, pytest.raises(PaginationError, match="total changed"):
        client.fetch_results(2024)


def test_race_twice_in_the_schedule_fails() -> None:
    def duplicate_first(payload: dict[str, Any]) -> dict[str, Any]:
        races = payload["MRData"]["RaceTable"]["Races"]
        races.insert(1, copy.deepcopy(races[0]))
        payload["MRData"]["total"] = "25"
        payload["MRData"]["limit"] = "25"
        return payload

    api = FakeJolpica(tamper={0: duplicate_first})
    with api.client() as client, pytest.raises(PaginationError, match="appears twice"):
        client.fetch_races(2024)


@pytest.mark.parametrize("page_size", [0, 101])
def test_invalid_page_size_is_rejected(page_size: int) -> None:
    with pytest.raises(ValueError, match="page_size"):
        FakeJolpica().client(page_size)


def test_from_settings_sends_the_user_agent_to_the_configured_base_url() -> None:
    api = FakeJolpica()
    settings = Settings(jolpica_base_url="https://mirror.example.test/f1", jolpica_user_agent="t/1")
    transport = httpx.MockTransport(api.handler)
    with JolpicaClient.from_settings(settings, transport=transport) as client:
        assert client.fetch_results(1900) == []
    request = api.requests[0]
    assert request.headers["user-agent"] == "t/1"
    assert str(request.url).startswith("https://mirror.example.test/f1/1900/results.json")
