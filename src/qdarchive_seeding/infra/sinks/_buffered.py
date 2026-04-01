"""Mixin providing buffered upsert with periodic flush for file-based sinks."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from qdarchive_seeding.infra.sinks.base import FLUSH_INTERVAL, BaseSink


@dataclass(slots=True)
class BufferedSink(BaseSink):
    """Base class for sinks that buffer records in memory and flush periodically.

    Subclasses must implement ``_flush_datasets()`` and ``_flush_assets()``
    to persist the buffered data to their backing store.
    """

    _dataset_buffer: dict[str, Any] = field(default_factory=dict, init=False, repr=False)
    _asset_buffer: dict[str, Any] = field(default_factory=dict, init=False, repr=False)
    _dataset_ops: int = field(default=0, init=False, repr=False)
    _asset_ops: int = field(default=0, init=False, repr=False)

    def _buffer_dataset(self, key: str, value: Any) -> str:
        """Buffer a dataset record and flush if threshold reached. Returns key."""
        self._dataset_buffer[key] = value
        self._dataset_ops += 1
        if self._dataset_ops >= FLUSH_INTERVAL:
            self._flush_datasets()
        return key

    def _buffer_asset(self, key: str, value: Any) -> None:
        """Buffer an asset record and flush if threshold reached."""
        self._asset_buffer[key] = value
        self._asset_ops += 1
        if self._asset_ops >= FLUSH_INTERVAL:
            self._flush_assets()

    def _flush_datasets(self) -> None:
        raise NotImplementedError

    def _flush_assets(self) -> None:
        raise NotImplementedError

    def close(self) -> None:
        self._flush_datasets()
        self._flush_assets()
