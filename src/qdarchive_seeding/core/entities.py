from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4


@dataclass(slots=True)
class DatasetRecord:
    source_name: str
    source_dataset_id: str | None
    source_url: str
    title: str | None = None
    description: str | None = None
    doi: str | None = None
    license: str | None = None
    year: int | None = None
    owner_name: str | None = None
    owner_email: str | None = None
    assets: list[AssetRecord] = field(default_factory=list)
    raw: dict[str, Any] | None = None


@dataclass(slots=True)
class AssetRecord:
    asset_url: str
    asset_type: str | None = None
    local_dir: str | None = None
    local_filename: str | None = None
    downloaded_at: datetime | None = None
    checksum_sha256: str | None = None
    size_bytes: int | None = None
    download_status: str | None = None
    error_message: str | None = None
    metadata: dict[str, Any] | None = None


@dataclass(slots=True)
class RunInfo:
    run_id: str = field(default_factory=lambda: str(uuid4()))
    pipeline_id: str | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    ended_at: datetime | None = None
    config_hash: str | None = None
    counts: dict[str, int] = field(default_factory=dict)
    failures: list[dict[str, Any]] = field(default_factory=list)
    environment: dict[str, Any] = field(default_factory=dict)
