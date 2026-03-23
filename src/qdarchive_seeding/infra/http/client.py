from __future__ import annotations

import logging
import time
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

logger = logging.getLogger(__name__)


def _is_retryable(exc: BaseException) -> bool:
    """Retry on connection errors and 5xx server errors (429 handled separately)."""
    if isinstance(exc, httpx.RequestError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    return False


@dataclass(slots=True)
class HttpClientSettings:
    timeout_seconds: float = 30.0
    max_retries: int = 3
    backoff_min: float = 0.5
    backoff_max: float = 6.0
    user_agent: str = "qdarchive-seeding/0.1"


class _IPv4Transport(httpx.HTTPTransport):
    """Force IPv4 connections to avoid IPv6 hangs on dual-stack hosts."""

    def __init__(self, **kwargs: Any) -> None:
        # httpcore's connection pool accepts local_address to bind to IPv4
        super().__init__(local_address="0.0.0.0", **kwargs)


class HttpxClient(HttpClient):
    def __init__(
        self,
        settings: HttpClientSettings,
        rate_limiter: Any | None = None,
    ) -> None:
        self._settings = settings
        self._rate_limiter = rate_limiter
        self._client = httpx.Client(
            timeout=settings.timeout_seconds,
            headers={"User-Agent": settings.user_agent},
            follow_redirects=True,
            transport=_IPv4Transport(),
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
            if self._rate_limiter is not None:
                self._rate_limiter.wait()
            combined_headers = {**self._client.headers, **headers}
            resp = self._client.get(url, headers=combined_headers, params=params, timeout=timeout)
            # Handle 429 inline: wait and re-send (up to 3 times) without
            # consuming tenacity retries, which are reserved for 5xx/network.
            for _attempt in range(3):
                if resp.status_code != 429:
                    break
                retry_after = resp.headers.get("retry-after", "")
                wait_secs = int(retry_after) if retry_after.isdigit() else 60
                logger.warning("Rate limited (429), waiting %ds before retry", wait_secs)
                time.sleep(wait_secs)
                if self._rate_limiter is not None:
                    self._rate_limiter.wait()
                resp = self._client.get(
                    url, headers=combined_headers, params=params, timeout=timeout
                )
            resp.raise_for_status()
            return resp

        return _request()

    def close(self) -> None:
        self._client.close()
