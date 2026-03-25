from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field


@dataclass(slots=True)
class RateLimiter:
    max_per_second: float
    last_request_time: float = 0.0
    _async_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)

    def wait(self) -> None:
        if self.max_per_second <= 0:
            return
        min_interval = 1.0 / self.max_per_second
        now = time.monotonic()
        elapsed = now - self.last_request_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self.last_request_time = time.monotonic()

    async def async_wait(self) -> None:
        async with self._async_lock:
            if self.max_per_second <= 0:
                return
            min_interval = 1.0 / self.max_per_second
            now = time.monotonic()
            elapsed = now - self.last_request_time
            if elapsed < min_interval:
                await asyncio.sleep(min_interval - elapsed)
            self.last_request_time = time.monotonic()
