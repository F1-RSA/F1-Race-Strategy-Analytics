import random
from collections.abc import Callable
from datetime import UTC, datetime

import httpx
import pytest

from f1_ingest.http import (
    HttpClient,
    HttpError,
    RetriesExhaustedError,
    TokenBucket,
    parse_retry_after,
)

Step = httpx.Response | Exception


class MaxJitter(random.Random):
    """Always picks the upper bound, so backoff delays are deterministic."""

    def uniform(self, a: float, b: float) -> float:
        return b


class Recorder:
    """Records sleeps without waiting, and serves scripted responses in order."""

    def __init__(self, steps: list[Step]) -> None:
        self.steps = steps
        self.requests: list[httpx.Request] = []
        self.sleeps: list[float] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        step = self.steps[len(self.requests) - 1]
        if isinstance(step, Exception):
            raise step
        return step

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)


def make_client(
    steps: list[Step],
    max_retries: int = 3,
    limiter_factory: Callable[[Recorder], TokenBucket] | None = None,
) -> tuple[HttpClient, Recorder]:
    rec = Recorder(steps)
    limiter = (
        limiter_factory(rec)
        if limiter_factory
        else TokenBucket(1000, 1000, clock=lambda: 0.0, sleep=rec.sleep)
    )
    client = HttpClient(
        "https://api.example.test/v1",
        limiter,
        timeout=5,
        max_retries=max_retries,
        backoff_base=1,
        backoff_max=5,
        transport=httpx.MockTransport(rec.handler),
        sleep=rec.sleep,
        rng=MaxJitter(),
    )
    return client, rec


def ok(payload: object = None) -> httpx.Response:
    return httpx.Response(200, json=payload if payload is not None else [{"a": 1}])


def test_returns_parsed_json_and_sends_params_relative_to_base_url() -> None:
    client, rec = make_client([ok([{"session_key": 9472}])])
    assert client.get_json("laps", {"session_key": 9472}) == [{"session_key": 9472}]
    assert str(rec.requests[0].url) == "https://api.example.test/v1/laps?session_key=9472"
    assert rec.sleeps == []


def test_429_waits_for_retry_after_then_succeeds() -> None:
    client, rec = make_client([httpx.Response(429, headers={"Retry-After": "7"}), ok()])
    assert client.get_json("laps") == [{"a": 1}]
    assert rec.sleeps == [7.0]
    assert len(rec.requests) == 2


def test_429_without_retry_after_uses_backoff() -> None:
    client, rec = make_client([httpx.Response(429), ok()])
    client.get_json("laps")
    assert rec.sleeps == [1.0]


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_5xx_is_retried(status: int) -> None:
    client, rec = make_client([httpx.Response(status), ok()])
    assert client.get_json("laps") == [{"a": 1}]
    assert len(rec.requests) == 2


def test_backoff_doubles_and_is_capped() -> None:
    client, rec = make_client([httpx.Response(503)] * 5 + [ok()], max_retries=5)
    client.get_json("laps")
    # base 1 * 2^n, capped at 5
    assert rec.sleeps == [1.0, 2.0, 4.0, 5.0, 5.0]


def test_jitter_stays_within_the_backoff_bound() -> None:
    client, rec = make_client([httpx.Response(503)] * 3 + [ok()])
    client._rng = random.Random(42)
    client.get_json("laps")
    for attempt, delay in enumerate(rec.sleeps):
        assert 0 <= delay <= min(5, 2**attempt)


def test_timeout_is_retried() -> None:
    client, rec = make_client([httpx.ReadTimeout("slow"), ok()])
    assert client.get_json("laps") == [{"a": 1}]
    assert rec.sleeps == [1.0]


def test_connection_error_is_retried() -> None:
    client, rec = make_client([httpx.ConnectError("refused"), ok()])
    assert client.get_json("laps") == [{"a": 1}]


def test_retries_exhausted_on_5xx_raises_with_last_status() -> None:
    client, rec = make_client([httpx.Response(503)] * 4, max_retries=3)
    with pytest.raises(RetriesExhaustedError, match="after 4 attempts: HTTP 503") as info:
        client.get_json("laps", {"session_key": 1})
    assert info.value.status_code == 503
    assert len(rec.requests) == 4
    assert rec.sleeps == [1.0, 2.0, 4.0]  # no sleep after the last attempt


def test_retries_exhausted_on_timeouts_raises() -> None:
    client, rec = make_client([httpx.ReadTimeout("slow")] * 3, max_retries=2)
    with pytest.raises(RetriesExhaustedError, match="ReadTimeout") as info:
        client.get_json("laps")
    assert info.value.status_code is None
    assert len(rec.requests) == 3


def test_zero_retries_fails_on_first_error() -> None:
    client, rec = make_client([httpx.Response(429)], max_retries=0)
    with pytest.raises(RetriesExhaustedError):
        client.get_json("laps")
    assert rec.sleeps == []


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_other_4xx_fail_immediately_with_body(status: int) -> None:
    client, rec = make_client([httpx.Response(status, json={"detail": "nope"})])
    with pytest.raises(HttpError) as info:
        client.get_json("laps")
    assert not isinstance(info.value, RetriesExhaustedError)
    assert info.value.status_code == status
    assert '"detail"' in info.value.body
    assert len(rec.requests) == 1


def test_every_attempt_takes_a_token_from_the_limiter() -> None:
    def slow_limiter(rec: Recorder) -> TokenBucket:
        clock = [0.0]

        def sleep(seconds: float) -> None:
            rec.sleep(seconds)
            clock[0] += seconds

        return TokenBucket(0.5, 1, clock=lambda: clock[0], sleep=sleep)

    client, rec = make_client(
        [httpx.Response(429, headers={"Retry-After": "0"}), ok()],
        limiter_factory=slow_limiter,
    )
    client.get_json("laps")
    # Retry-After 0, then the limiter still enforces 2 s between the two attempts.
    assert rec.sleeps == [0.0, 2.0]


NOW = datetime(2024, 3, 2, 15, 0, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        (None, None),
        ("120", 120.0),
        (" 3 ", 3.0),
        ("Sat, 02 Mar 2024 15:00:30 GMT", 30.0),
        ("Sat, 02 Mar 2024 14:59:00 GMT", 0.0),  # date in the past: retry now
        ("soon", None),
        ("-5", None),
    ],
)
def test_parse_retry_after(header: str | None, expected: float | None) -> None:
    assert parse_retry_after(header, NOW) == expected


def test_custom_headers_are_sent_next_to_accept() -> None:
    recorder = Recorder([httpx.Response(200, json=[])])
    http = HttpClient(
        "https://example.test/api",
        TokenBucket(10, 1),
        timeout=5,
        max_retries=0,
        backoff_base=1,
        backoff_max=1,
        headers={"User-Agent": "f1-test/1.0"},
        transport=httpx.MockTransport(recorder.handler),
    )
    http.get_json("races.json")
    headers = recorder.requests[0].headers
    assert headers["user-agent"] == "f1-test/1.0"
    assert headers["accept"] == "application/json"


def test_build_http_client_uses_the_given_api_settings() -> None:
    from f1_ingest.config import Settings
    from f1_ingest.http import build_http_client

    recorder = Recorder([httpx.Response(200, json={"ok": True})])
    http = build_http_client(
        Settings(),
        base_url="https://example.test/ergast/f1",
        requests_per_minute=6,
        burst=2,
        headers={"User-Agent": "f1-test/1.0"},
        transport=httpx.MockTransport(recorder.handler),
    )
    assert http.get_json("2024/races.json", {"limit": 100}) == {"ok": True}
    assert (
        str(recorder.requests[0].url) == "https://example.test/ergast/f1/2024/races.json?limit=100"
    )
