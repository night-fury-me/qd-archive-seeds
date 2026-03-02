from __future__ import annotations

from qdarchive_seeding.infra.http.rate_limit import RateLimiter


def test_rate_limiter_zero_rate_no_wait() -> None:
    limiter = RateLimiter(max_per_second=0.0, last_request_time=1.0)
    limiter.wait()
    assert limiter.last_request_time == 1.0


def test_rate_limiter_sleeps_and_updates_time(monkeypatch: object) -> None:
    limiter = RateLimiter(max_per_second=2.0, last_request_time=0.0)
    calls: list[float] = []
    times = iter([0.1, 0.6])

    def fake_monotonic() -> float:
        return next(times)

    def fake_sleep(seconds: float) -> None:
        calls.append(seconds)

    monkeypatch.setattr("qdarchive_seeding.infra.http.rate_limit.time.monotonic", fake_monotonic)
    monkeypatch.setattr("qdarchive_seeding.infra.http.rate_limit.time.sleep", fake_sleep)

    limiter.wait()

    assert calls == [0.4]
    assert limiter.last_request_time == 0.6
