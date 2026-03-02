from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

from qdarchive_seeding.core.interfaces import HttpClient


def _is_retryable(exc: BaseException) -> bool:
    """Retry on connection errors and 5xx server errors."""
    if isinstance(exc, httpx.RequestError):
        return True
    return isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code >= 500


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
        transport = httpx.HTTPTransport(local_address="0.0.0.0")
        self._client = httpx.Client(
            transport=transport,
            timeout=settings.timeout_seconds,
            headers={"User-Agent": settings.user_agent},
            follow_redirects=True,
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
            retry=retry_if_exception(_is_retryable),
            stop=stop_after_attempt(self._settings.max_retries),
            wait=wait_exponential_jitter(
                initial=self._settings.backoff_min, max=self._settings.backoff_max
            ),
        )
        def _request() -> httpx.Response:
            combined_headers = {**self._client.headers, **headers}
            resp = self._client.get(url, headers=combined_headers, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp

        return _request()

    def close(self) -> None:
        self._client.close()
