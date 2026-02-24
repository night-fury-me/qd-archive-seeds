from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass(slots=True)
class RateLimiter:
    max_per_second: float
    last_request_time: float = 0.0

    def wait(self) -> None:
        if self.max_per_second <= 0:
            return
        min_interval = 1.0 / self.max_per_second
        now = time.monotonic()
        elapsed = now - self.last_request_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self.last_request_time = time.monotonic()
