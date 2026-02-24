from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from qdarchive_seeding.core.interfaces import AuthProvider


@dataclass(slots=True)
class NoAuth(AuthProvider):
    def apply(self, headers: dict[str, str], params: dict[str, Any]) -> tuple[dict[str, str], dict[str, Any]]:
        return headers, params


@dataclass(slots=True)
class ApiKeyAuth(AuthProvider):
    api_key: str
    placement: str = "header"
    header_name: str = "X-API-Key"
    query_param: str = "api_key"

    def apply(self, headers: dict[str, str], params: dict[str, Any]) -> tuple[dict[str, str], dict[str, Any]]:
        if self.placement == "header":
            headers = {**headers, self.header_name: self.api_key}
            return headers, params
        params = {**params, self.query_param: self.api_key}
        return headers, params


@dataclass(slots=True)
class BearerAuth(AuthProvider):
    token: str

    def apply(self, headers: dict[str, str], params: dict[str, Any]) -> tuple[dict[str, str], dict[str, Any]]:
        headers = {**headers, "Authorization": f"Bearer {self.token}"}
        return headers, params
