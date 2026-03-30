from __future__ import annotations

import contextlib
import logging
from collections.abc import Callable
from dataclasses import dataclass

from qdarchive_seeding.core.entities import RunInfo

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class StageChanged:
    stage: str  # "extract" | "transform" | "download" | "load" | "done"


@dataclass(frozen=True, slots=True)
class QueryProgress:
    """Tracks which query is currently running and total query count."""

    current_query: int = 0
    total_queries: int = 0
    query_label: str = ""
    query_type: str = ""  # "extension" | "nl"


@dataclass(frozen=True, slots=True)
class PageProgress:
    """Tracks pagination progress within a single query."""

    current_page: int = 0
    total_hits: int = 0  # from API response, 0 if unknown
    query_label: str = ""


@dataclass(frozen=True, slots=True)
class DateSliceProgress:
    """Tracks date-range splitting progress when a query exceeds the 10k limit."""

    current_slice: int = 0
    total_slices: int = 0
    query_label: str = ""
    slice_label: str = ""  # e.g. "[2021-01-01→2021-06-30]"


@dataclass(frozen=True, slots=True)
class CountersUpdated:
    extracted: int = 0
    transformed: int = 0
    downloaded: int = 0
    failed: int = 0
    skipped: int = 0
    access_denied: int = 0
    total_assets: int = 0


@dataclass(frozen=True, slots=True)
class AssetDownloadUpdate:
    asset_url: str
    status: str
    bytes_downloaded: int = 0
    total_bytes: int | None = None


@dataclass(frozen=True, slots=True)
class AssetDownloadProgress:
    """Emitted during streaming to report byte-level progress for the current asset."""

    asset_url: str
    bytes_downloaded: int
    total_bytes: int | None = None


@dataclass(frozen=True, slots=True)
class ErrorEvent:
    component: str
    error_type: str
    message: str
    asset_url: str | None = None


@dataclass(frozen=True, slots=True)
class MetadataCollected:
    """Emitted after Phase 1 (metadata collection) completes."""

    total_projects: int = 0
    total_files: int = 0
    total_size_bytes: int = 0
    queries_run: int = 0


@dataclass(frozen=True, slots=True)
class Completed:
    run_info: RunInfo


ProgressEvent = (
    StageChanged
    | QueryProgress
    | PageProgress
    | DateSliceProgress
    | CountersUpdated
    | AssetDownloadUpdate
    | AssetDownloadProgress
    | ErrorEvent
    | MetadataCollected
    | Completed
)

Callback = Callable[[ProgressEvent], None]


class ProgressBus:
    def __init__(self) -> None:
        self._subscribers: list[Callback] = []

    def publish(self, event: ProgressEvent) -> None:
        for callback in self._subscribers:
            try:
                callback(event)
            except Exception:
                logger.debug("Subscriber exception ignored", exc_info=True)

    def subscribe(self, callback: Callback) -> Callable[[], None]:
        self._subscribers.append(callback)

        def unsubscribe() -> None:
            with contextlib.suppress(ValueError):
                self._subscribers.remove(callback)

        return unsubscribe
