from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from qdarchive_seeding.core.interfaces import AuthProvider


@dataclass(slots=True)
class NoAuth(AuthProvider):
    def apply(
        self, headers: dict[str, str], params: dict[str, Any]
    ) -> tuple[dict[str, str], dict[str, Any]]:
        return headers, params


@dataclass(slots=True)
class ApiKeyAuth(AuthProvider):
    api_key: str
    placement: str = "header"
    header_name: str = "X-API-Key"
    query_param: str = "api_key"

    def apply(
        self, headers: dict[str, str], params: dict[str, Any]
    ) -> tuple[dict[str, str], dict[str, Any]]:
        if self.placement == "header":
            headers = {**headers, self.header_name: self.api_key}
            return headers, params
        params = {**params, self.query_param: self.api_key}
        return headers, params


@dataclass(slots=True)
class BearerAuth(AuthProvider):
    token: str

    def apply(
        self, headers: dict[str, str], params: dict[str, Any]
    ) -> tuple[dict[str, str], dict[str, Any]]:
        headers = {**headers, "Authorization": f"Bearer {self.token}"}
        return headers, params


@dataclass(slots=True)
class OAuth2ClientCredentials(AuthProvider):
    token_url: str
    client_id: str
    client_secret: str
    scope: str = ""
    _token: str | None = field(default=None, repr=False)
    _expires_at: float = field(default=0.0, repr=False)

    def apply(
        self, headers: dict[str, str], params: dict[str, Any]
    ) -> tuple[dict[str, str], dict[str, Any]]:
        token = self._get_token()
        headers = {**headers, "Authorization": f"Bearer {token}"}
        return headers, params

    def _get_token(self) -> str:
        if self._token and time.monotonic() < self._expires_at:
            return self._token
        return self._fetch_token()

    def _fetch_token(self) -> str:
        data: dict[str, str] = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        if self.scope:
            data["scope"] = self.scope

        with httpx.Client() as client:
            response = client.post(self.token_url, data=data)
            response.raise_for_status()
            payload = response.json()

        self._token = payload["access_token"]
        expires_in = payload.get("expires_in", 3600)
        self._expires_at = time.monotonic() + expires_in - 10
        assert self._token is not None
        return self._token
