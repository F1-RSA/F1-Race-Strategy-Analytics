import json
import random
from pathlib import Path

import httpx
import pytest

from f1_ingest.config import Settings
from f1_ingest.http import HttpClient, HttpError, RetriesExhaustedError, TokenBucket
from f1_ingest.openf1 import ENDPOINTS, OpenF1Client

FIXTURES = Path(__file__).parent / "fixtures" / "openf1"
SESSION_KEY = 9472
BASE_URL = "https://api.openf1.org/v1"


def fixture(endpoint: str) -> list[dict]:
    return json.loads((FIXTURES / f"session_{SESSION_KEY}" / f"{endpoint}.json").read_text())


NO_RESULTS = (FIXTURES / "no_results.json").read_bytes()


class FakeOpenF1:
    """Mocked transport that answers like the real API, from the recorded fixtures."""

    def __init__(self, overrides: dict[str, list[httpx.Response | Exception]] | None = None):
        self.overrides = overrides or {}
        self.requests: list[httpx.Request] = []
        self.sleeps: list[float] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        endpoint = request.url.path.rsplit("/", 1)[-1]
        if self.overrides.get(endpoint):
            step = self.overrides[endpoint].pop(0)
            if isinstance(step, Exception):
                raise step
            return step
        path = FIXTURES / f"session_{request.url.params.get('session_key')}" / f"{endpoint}.json"
        if not path.exists():
            return httpx.Response(404, content=NO_RESULTS)
        return httpx.Response(200, content=path.read_bytes())

    def client(self, max_retries: int = 2) -> OpenF1Client:
        clock = [0.0]

        def sleep(seconds: float) -> None:
            self.sleeps.append(seconds)
            clock[0] += seconds

        http = HttpClient(
            BASE_URL,
            TokenBucket(0.4, 2, clock=lambda: clock[0], sleep=sleep),
            timeout=5,
            max_retries=max_retries,
            backoff_base=1,
            backoff_max=60,
            transport=httpx.MockTransport(self.handler),
            sleep=sleep,
            rng=random.Random(0),
        )
        return OpenF1Client(http)


def test_fetch_session_returns_all_endpoints_unchanged() -> None:
    api = FakeOpenF1()
    with api.client() as client:
        data = client.fetch_session(SESSION_KEY)
    assert list(data) == list(ENDPOINTS)
    for endpoint in ENDPOINTS:
        assert data[endpoint] == fixture(endpoint)
    counts = {endpoint: len(records) for endpoint, records in data.items()}
    assert counts == {"sessions": 1, "drivers": 20, "laps": 114, "stints": 63, "pit": 43}


def test_one_request_per_endpoint_filtered_by_session_key() -> None:
    api = FakeOpenF1()
    with api.client() as client:
        client.fetch_session(SESSION_KEY)
    assert [str(r.url) for r in api.requests] == [
        f"{BASE_URL}/{endpoint}?session_key={SESSION_KEY}" for endpoint in ENDPOINTS
    ]


def test_session_is_fetched_within_the_rate_limit() -> None:
    api = FakeOpenF1()
    with api.client() as client:
        client.fetch_session(SESSION_KEY)
    # Burst of 2, then one request every 2.5 s at 24 requests per minute.
    assert api.sleeps == [pytest.approx(2.5)] * 3


def test_pit_keeps_deprecated_and_new_duration_fields() -> None:
    with FakeOpenF1().client() as client:
        pit = client.fetch("pit", SESSION_KEY)
    assert {"pit_duration", "lane_duration", "stop_duration"} <= pit[0].keys()


def test_empty_result_404_becomes_empty_list() -> None:
    api = FakeOpenF1()
    with api.client() as client:
        assert client.fetch("laps", 1) == []
    assert len(api.requests) == 1


def test_other_404_is_raised() -> None:
    api = FakeOpenF1({"laps": [httpx.Response(404, text="<html>Not Found</html>")]})
    with api.client() as client, pytest.raises(HttpError) as info:
        client.fetch("laps", SESSION_KEY)
    assert info.value.status_code == 404


def test_unknown_endpoint_is_rejected_without_a_request() -> None:
    api = FakeOpenF1()
    with api.client() as client, pytest.raises(ValueError, match="weather"):
        client.fetch("weather", SESSION_KEY)
    assert api.requests == []


def test_non_list_payload_is_rejected() -> None:
    api = FakeOpenF1({"laps": [httpx.Response(200, json={"unexpected": True})]})
    with api.client() as client, pytest.raises(ValueError, match="expected a list"):
        client.fetch("laps", SESSION_KEY)


def test_429_with_retry_after_is_retried() -> None:
    api = FakeOpenF1({"laps": [httpx.Response(429, headers={"Retry-After": "10"})]})
    with api.client() as client:
        assert client.fetch("laps", SESSION_KEY) == fixture("laps")
    assert 10.0 in api.sleeps
    assert len(api.requests) == 2


def test_5xx_and_timeout_are_retried() -> None:
    api = FakeOpenF1({"stints": [httpx.Response(502), httpx.ReadTimeout("slow")]})
    with api.client() as client:
        assert client.fetch("stints", SESSION_KEY) == fixture("stints")
    assert len(api.requests) == 3


def test_exhausted_retries_fail_the_whole_session() -> None:
    api = FakeOpenF1({"stints": [httpx.Response(503)] * 3})
    with api.client(max_retries=2) as client, pytest.raises(RetriesExhaustedError):
        client.fetch_session(SESSION_KEY)
    # sessions, drivers and laps succeeded, stints failed 3 times, pit was never requested.
    paths = [r.url.path.rsplit("/", 1)[-1] for r in api.requests]
    assert paths == ["sessions", "drivers", "laps", "stints", "stints", "stints"]


def test_from_settings_wires_rate_and_base_url() -> None:
    api = FakeOpenF1()
    settings = Settings(openf1_base_url="https://mirror.example.test/v1")
    with OpenF1Client.from_settings(settings, transport=httpx.MockTransport(api.handler)) as c:
        assert c.fetch("sessions", SESSION_KEY) == fixture("sessions")
    assert str(api.requests[0].url).startswith("https://mirror.example.test/v1/sessions")


def test_list_sessions_filters_by_year_and_returns_records_unchanged() -> None:
    listed = [*fixture("sessions"), {"session_key": 9480, "meeting_key": 1230, "extra": [1, 2]}]
    api = FakeOpenF1({"sessions": [httpx.Response(200, json=listed)]})
    with api.client() as client:
        assert client.list_sessions(2024) == listed
    assert [str(r.url) for r in api.requests] == [f"{BASE_URL}/sessions?year=2024"]


def test_list_sessions_without_results_is_empty() -> None:
    api = FakeOpenF1({"sessions": [httpx.Response(404, content=NO_RESULTS)]})
    with api.client() as client:
        assert client.list_sessions(1999) == []
