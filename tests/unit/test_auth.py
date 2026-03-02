from __future__ import annotations

from qdarchive_seeding.infra.http.auth import (
    ApiKeyAuth,
    BearerAuth,
    NoAuth,
    OAuth2ClientCredentials,
)


def test_no_auth_passthrough() -> None:
    auth = NoAuth()
    headers, params = auth.apply({"existing": "header"}, {"key": "val"})
    assert headers == {"existing": "header"}
    assert params == {"key": "val"}


def test_api_key_auth_header() -> None:
    auth = ApiKeyAuth(api_key="secret123", placement="header", header_name="X-API-Key")
    headers, params = auth.apply({}, {})
    assert headers["X-API-Key"] == "secret123"
    assert "api_key" not in params


def test_api_key_auth_query() -> None:
    auth = ApiKeyAuth(api_key="secret123", placement="query", query_param="token")
    headers, params = auth.apply({}, {})
    assert "X-API-Key" not in headers
    assert params["token"] == "secret123"


def test_bearer_auth() -> None:
    auth = BearerAuth(token="mytoken")
    headers, params = auth.apply({}, {})
    assert headers["Authorization"] == "Bearer mytoken"


def test_oauth2_client_credentials_fetches_and_caches_token(monkeypatch: object) -> None:
    calls: list[dict[str, object]] = []

    class DummyResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"access_token": "token-123", "expires_in": 100}

    class DummyClient:
        def __enter__(self) -> "DummyClient":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def post(self, url: str, data: dict[str, str]) -> DummyResponse:
            calls.append({"url": url, "data": data})
            return DummyResponse()

    monkeypatch.setattr("qdarchive_seeding.infra.http.auth.httpx.Client", DummyClient)
    monkeypatch.setattr("qdarchive_seeding.infra.http.auth.time.monotonic", lambda: 0.0)

    auth = OAuth2ClientCredentials(
        token_url="https://example.com/token",
        client_id="client",
        client_secret="secret",
        scope="read",
    )

    headers, _ = auth.apply({}, {})
    headers2, _ = auth.apply({}, {})

    assert headers["Authorization"] == "Bearer token-123"
    assert headers2["Authorization"] == "Bearer token-123"
    assert len(calls) == 1
    assert calls[0]["url"] == "https://example.com/token"
    assert calls[0]["data"]["scope"] == "read"
