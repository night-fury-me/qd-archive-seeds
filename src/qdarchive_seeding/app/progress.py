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
class CountersUpdated:
    extracted: int = 0
    transformed: int = 0
    downloaded: int = 0
    failed: int = 0
    skipped: int = 0
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
class Completed:
    run_info: RunInfo


ProgressEvent = (
    StageChanged
    | CountersUpdated
    | AssetDownloadUpdate
    | AssetDownloadProgress
    | ErrorEvent
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
