from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Union

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


@dataclass(frozen=True, slots=True)
class AssetDownloadUpdate:
    asset_url: str
    status: str
    bytes_downloaded: int = 0
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


ProgressEvent = Union[StageChanged, CountersUpdated, AssetDownloadUpdate, ErrorEvent, Completed]

Callback = Callable[[ProgressEvent], None]


class ProgressBus:
    def __init__(self) -> None:
        self._subscribers: list[Callback] = []
        self._lock = threading.Lock()

    def publish(self, event: ProgressEvent) -> None:
        with self._lock:
            subscribers = list(self._subscribers)
        for callback in subscribers:
            try:
                callback(event)
            except Exception:
                logger.debug("Subscriber exception ignored", exc_info=True)

    def subscribe(self, callback: Callback) -> Callable[[], None]:
        with self._lock:
            self._subscribers.append(callback)

        def unsubscribe() -> None:
            with self._lock:
                try:
                    self._subscribers.remove(callback)
                except ValueError:
                    pass

        return unsubscribe
