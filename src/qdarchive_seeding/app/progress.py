"""Backward-compatibility shim — all definitions live in core.progress."""

from __future__ import annotations

from qdarchive_seeding.core.progress import (
    AssetDownloadProgress,
    AssetDownloadStarted,
    AssetDownloadUpdate,
    Callback,
    Completed,
    CountersUpdated,
    DateSliceProgress,
    ErrorEvent,
    MetadataCollected,
    PageProgress,
    ProgressBus,
    ProgressEvent,
    QueryProgress,
    StageChanged,
)

__all__ = [
    "AssetDownloadProgress",
    "AssetDownloadStarted",
    "AssetDownloadUpdate",
    "Callback",
    "Completed",
    "CountersUpdated",
    "DateSliceProgress",
    "ErrorEvent",
    "MetadataCollected",
    "PageProgress",
    "ProgressBus",
    "ProgressEvent",
    "QueryProgress",
    "StageChanged",
]
