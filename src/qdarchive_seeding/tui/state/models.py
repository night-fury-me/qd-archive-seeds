from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class UIRunRecord:
    run_id: str
    pipeline_id: str
    started_at: str
    ended_at: str | None = None
    counts: dict[str, int] = field(default_factory=dict)
    failures_count: int = 0


@dataclass(slots=True)
class UIDownloadEntry:
    asset_url: str
    status: str
    bytes_downloaded: int = 0
    total_bytes: int | None = None
    error: str | None = None
