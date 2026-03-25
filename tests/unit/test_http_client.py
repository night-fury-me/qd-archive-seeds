from __future__ import annotations

import httpx
import pytest
import respx
from tenacity import RetryError

from qdarchive_seeding.infra.http.client import HttpClientSettings, HttpxClient
from qdarchive_seeding.infra.http.rate_limit import RateLimiter


@pytest.fixture()
def fast_settings() -> HttpClientSettings:
    """Settings with zero backoff so retries don't slow tests."""
    return HttpClientSettings(
        timeout_seconds=5.0,
        max_retries=3,
        backoff_min=0.0,
        backoff_max=0.0,
        user_agent="test-agent/0.0",
    )


@pytest.fixture()
def client(fast_settings: HttpClientSettings) -> HttpxClient:
    c = HttpxClient(fast_settings)
    yield c  # type: ignore[misc]
    c.close()


# ---------- 1. Successful GET request ----------


@respx.mock
def test_successful_get_returns_response(client: HttpxClient) -> None:
    route = respx.get("https://example.com/api").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )

    resp = client.get("https://example.com/api", headers={}, params={})

    assert resp.status_code == 200
    assert resp.json() == {"ok": True}
    assert route.call_count == 1


# ---------- 2. 500 errors are retried ----------


@respx.mock
def test_500_is_retried(client: HttpxClient) -> None:
    route = respx.get("https://example.com/api").mock(
        return_value=httpx.Response(500, text="Internal Server Error")
    )

    with pytest.raises(RetryError) as exc_info:
        client.get("https://example.com/api", headers={}, params={})

    last_exc = exc_info.value.last_attempt.exception()
    assert isinstance(last_exc, httpx.HTTPStatusError)
    assert last_exc.response.status_code == 500
    assert route.call_count == 3


@respx.mock
def test_500_then_success(client: HttpxClient) -> None:
    """Server fails twice then succeeds on the third attempt."""
    route = respx.get("https://example.com/api").mock(
        side_effect=[
            httpx.Response(500, text="error"),
            httpx.Response(500, text="error"),
            httpx.Response(200, json={"recovered": True}),
        ]
    )

    resp = client.get("https://example.com/api", headers={}, params={})

    assert resp.status_code == 200
    assert resp.json() == {"recovered": True}
    assert route.call_count == 3


# ---------- 3. 4xx errors are NOT retried ----------


@respx.mock
def test_404_not_retried(client: HttpxClient) -> None:
    route = respx.get("https://example.com/api").mock(
        return_value=httpx.Response(404, text="Not Found")
    )

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        client.get("https://example.com/api", headers={}, params={})

    assert exc_info.value.response.status_code == 404
    assert route.call_count == 1


@respx.mock
def test_403_not_retried(client: HttpxClient) -> None:
    route = respx.get("https://example.com/api").mock(
        return_value=httpx.Response(403, text="Forbidden")
    )

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        client.get("https://example.com/api", headers={}, params={})

    assert exc_info.value.response.status_code == 403
    assert route.call_count == 1


# ---------- 4. Connection errors are retried ----------


@respx.mock
def test_connect_error_is_retried(client: HttpxClient) -> None:
    route = respx.get("https://example.com/api").mock(
        side_effect=httpx.ConnectError("connection refused")
    )

    with pytest.raises(RetryError) as exc_info:
        client.get("https://example.com/api", headers={}, params={})

    last_exc = exc_info.value.last_attempt.exception()
    assert isinstance(last_exc, httpx.ConnectError)
    assert route.call_count == 3


@respx.mock
def test_connect_error_then_success(client: HttpxClient) -> None:
    route = respx.get("https://example.com/api").mock(
        side_effect=[
            httpx.ConnectError("connection refused"),
            httpx.Response(200, json={"ok": True}),
        ]
    )

    resp = client.get("https://example.com/api", headers={}, params={})

    assert resp.status_code == 200
    assert route.call_count == 2


# ---------- 5. Custom settings are applied ----------


def test_custom_user_agent() -> None:
    settings = HttpClientSettings(user_agent="custom-ua/1.0")
    c = HttpxClient(settings)
    try:
        assert c._client.headers["User-Agent"] == "custom-ua/1.0"
    finally:
        c.close()


def test_custom_timeout() -> None:
    settings = HttpClientSettings(timeout_seconds=42.0)
    c = HttpxClient(settings)
    try:
        assert c._client.timeout.connect == 42.0
        assert c._client.timeout.read == 42.0
    finally:
        c.close()


@respx.mock
def test_per_request_timeout_forwarded(client: HttpxClient) -> None:
    """Ensure per-request timeout parameter is passed through."""
    route = respx.get("https://example.com/api").mock(return_value=httpx.Response(200, text="ok"))

    resp = client.get("https://example.com/api", headers={}, params={}, timeout=1.0)

    assert resp.status_code == 200
    assert route.call_count == 1


@respx.mock
def test_custom_headers_merged(client: HttpxClient) -> None:
    """Extra headers passed to get() are merged with the client defaults."""
    route = respx.get("https://example.com/api").mock(return_value=httpx.Response(200, text="ok"))

    client.get(
        "https://example.com/api",
        headers={"X-Custom": "value"},
        params={},
    )

    sent_request = route.calls[0].request
    assert sent_request.headers["X-Custom"] == "value"
    assert sent_request.headers["User-Agent"] == "test-agent/0.0"


def test_default_settings_values() -> None:
    settings = HttpClientSettings()
    assert settings.timeout_seconds == 30.0
    assert settings.max_retries == 3
    assert settings.backoff_min == 0.5
    assert settings.backoff_max == 6.0
    assert settings.user_agent == "qdarchive-seeding/0.1"


# ---------- 6. get_many concurrent fetching ----------


@respx.mock
def test_get_many_returns_all_responses(client: HttpxClient) -> None:
    """get_many fetches multiple URLs concurrently."""
    respx.get("https://example.com/api/1").mock(
        return_value=httpx.Response(200, json={"id": 1})
    )
    respx.get("https://example.com/api/2").mock(
        return_value=httpx.Response(200, json={"id": 2})
    )
    respx.get("https://example.com/api/3").mock(
        return_value=httpx.Response(200, json={"id": 3})
    )

    responses = client.get_many([
        {"url": "https://example.com/api/1", "headers": {}, "params": {}},
        {"url": "https://example.com/api/2", "headers": {}, "params": {}},
        {"url": "https://example.com/api/3", "headers": {}, "params": {}},
    ])

    assert len(responses) == 3
    ids = {r.json()["id"] for r in responses}
    assert ids == {1, 2, 3}


@respx.mock
def test_get_many_with_rate_limiter(fast_settings: HttpClientSettings) -> None:
    """get_many respects rate limiter."""
    limiter = RateLimiter(max_per_second=100.0)
    c = HttpxClient(fast_settings, rate_limiter=limiter)
    try:
        respx.get("https://example.com/api/a").mock(
            return_value=httpx.Response(200, json={"x": "a"})
        )
        respx.get("https://example.com/api/b").mock(
            return_value=httpx.Response(200, json={"x": "b"})
        )

        responses = c.get_many([
            {"url": "https://example.com/api/a", "headers": {}, "params": {}},
            {"url": "https://example.com/api/b", "headers": {}, "params": {}},
        ])

        assert len(responses) == 2
    finally:
        c.close()


@respx.mock
def test_get_many_empty_list(client: HttpxClient) -> None:
    """get_many with no requests returns empty list."""
    responses = client.get_many([])
    assert responses == []
