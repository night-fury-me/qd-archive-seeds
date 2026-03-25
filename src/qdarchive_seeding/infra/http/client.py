from __future__ import annotations

import asyncio
import logging
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
    """Retry on connection errors, 5xx server errors, and 429 rate limits."""
    if isinstance(exc, httpx.RequestError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500 or exc.response.status_code == 429
    return False


@dataclass(slots=True)
class HttpClientSettings:
    timeout_seconds: float = 30.0
    max_retries: int = 3
    backoff_min: float = 0.5
    backoff_max: float = 6.0
    user_agent: str = "qdarchive-seeding/0.1"


class _AsyncIPv4Transport(httpx.AsyncHTTPTransport):
    """Force IPv4 connections to avoid IPv6 hangs on dual-stack hosts."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(local_address="0.0.0.0", **kwargs)


class HttpxClient(HttpClient):
    def __init__(
        self,
        settings: HttpClientSettings,
        rate_limiter: Any | None = None,
    ) -> None:
        self._settings = settings
        self._rate_limiter = rate_limiter
        self._client = httpx.AsyncClient(
            timeout=settings.timeout_seconds,
            headers={"User-Agent": settings.user_agent},
            follow_redirects=True,
            transport=_AsyncIPv4Transport(),
        )

    async def get(
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
        async def _request() -> httpx.Response:
            if self._rate_limiter is not None:
                await self._rate_limiter.async_wait()
            combined_headers = {**self._client.headers, **headers}
            resp = await self._client.get(
                url, headers=combined_headers, params=params, timeout=timeout
            )
            for _attempt in range(8):
                if resp.status_code != 429:
                    break
                retry_after = resp.headers.get("retry-after", "")
                wait_secs = int(retry_after) if retry_after.isdigit() else 30
                logger.warning("Rate limited (429), waiting %ds before retry", wait_secs)
                await asyncio.sleep(wait_secs)
                if self._rate_limiter is not None:
                    await self._rate_limiter.async_wait()
                resp = await self._client.get(
                    url, headers=combined_headers, params=params, timeout=timeout
                )
            resp.raise_for_status()
            return resp

        return await _request()

    async def get_many(
        self,
        requests: list[dict[str, Any]],
    ) -> list[httpx.Response]:
        """Fetch multiple URLs concurrently, respecting rate limits."""
        tasks = [
            self.get(
                r["url"],
                headers=r.get("headers", {}),
                params=r.get("params", {}),
                timeout=r.get("timeout"),
            )
            for r in requests
        ]
        return await asyncio.gather(*tasks)

    async def close(self) -> None:
        await self._client.aclose()
