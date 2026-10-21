import pytest

from f1_ingest.http import TokenBucket


class FakeClock:
    """Virtual time: sleeping advances the clock instantly, so timing tests never wait."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def make_bucket(rate: float = 0.4, capacity: int = 2) -> tuple[TokenBucket, FakeClock]:
    clock = FakeClock()
    return TokenBucket(rate, capacity, clock=clock, sleep=clock.sleep), clock


def test_burst_passes_without_waiting() -> None:
    bucket, clock = make_bucket(capacity=2)
    assert bucket.acquire() == 0
    assert bucket.acquire() == 0
    assert clock.sleeps == []


def test_waits_one_interval_once_burst_is_used() -> None:
    bucket, clock = make_bucket(rate=0.4, capacity=2)
    bucket.acquire()
    bucket.acquire()
    assert bucket.acquire() == pytest.approx(2.5)
    assert clock.sleeps == [pytest.approx(2.5)]


def test_only_waits_for_the_missing_fraction_of_a_token() -> None:
    bucket, clock = make_bucket(rate=0.4, capacity=1)
    bucket.acquire()
    clock.now += 1.0  # 0.4 tokens refilled while idle
    assert bucket.acquire() == pytest.approx(1.5)


def test_idle_time_refills_only_up_to_capacity() -> None:
    bucket, clock = make_bucket(rate=0.4, capacity=2)
    bucket.acquire()
    bucket.acquire()
    clock.now += 3600
    assert bucket.acquire() == 0
    assert bucket.acquire() == 0
    assert bucket.acquire() == pytest.approx(2.5)


def test_never_exceeds_capacity_plus_rate_in_any_minute() -> None:
    rate, capacity = 0.4, 2
    bucket, clock = make_bucket(rate=rate, capacity=capacity)
    stamps = []
    for _ in range(200):
        bucket.acquire()
        stamps.append(clock.now)
    allowed = capacity + rate * 60  # 26 with the defaults, below an assumed 30/min limit
    for i, start in enumerate(stamps):
        in_window = sum(1 for t in stamps[i:] if t < start + 60)
        assert in_window <= allowed
    # Steady state is exactly the configured rate.
    assert stamps[-1] - stamps[-25] == pytest.approx(24 / rate)


@pytest.mark.parametrize(("rate", "capacity"), [(0, 1), (-1, 1), (1, 0)])
def test_invalid_parameters_are_rejected(rate: float, capacity: int) -> None:
    with pytest.raises(ValueError):
        TokenBucket(rate, capacity)
