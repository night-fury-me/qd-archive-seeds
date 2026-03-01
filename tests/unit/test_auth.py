from __future__ import annotations

from qdarchive_seeding.infra.http.auth import ApiKeyAuth, BearerAuth, NoAuth


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
