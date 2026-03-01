from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from qdarchive_seeding.core.interfaces import HttpClient


@dataclass(slots=True)
class HttpClientSettings:
    timeout_seconds: float = 30.0
    max_retries: int = 3
    backoff_min: float = 0.5
    backoff_max: float = 6.0
    user_agent: str = "qdarchive-seeding/0.1"


class HttpxClient(HttpClient):
    def __init__(self, settings: HttpClientSettings) -> None:
        self._settings = settings
        self._client = httpx.Client(
            timeout=settings.timeout_seconds, headers={"User-Agent": settings.user_agent}
        )

    def get(
        self,
        url: str,
        *,
        headers: dict[str, str],
        params: dict[str, Any],
        timeout: float | None = None,
    ) -> httpx.Response:
        @retry(
            retry=retry_if_exception_type(httpx.RequestError),
            stop=stop_after_attempt(self._settings.max_retries),
            wait=wait_exponential_jitter(
                min=self._settings.backoff_min, max=self._settings.backoff_max
            ),
        )
        def _request() -> httpx.Response:
            combined_headers = {**self._client.headers, **headers}
            return self._client.get(url, headers=combined_headers, params=params, timeout=timeout)

        return _request()

    def close(self) -> None:
        self._client.close()
