from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.infra.sinks.base import BaseSink


@dataclass(slots=True)
class CSVSink(BaseSink):
    dataset_path: Path
    asset_path: Path

    def __post_init__(self) -> None:
        self.dataset_path.parent.mkdir(parents=True, exist_ok=True)
        self.asset_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.dataset_path.exists():
            with self.dataset_path.open("w", newline="") as fh:
                writer = csv.writer(fh)
                writer.writerow(
                    [
                        "id",
                        "source_name",
                        "source_dataset_id",
                        "source_url",
                        "title",
                        "description",
                        "doi",
                        "license",
                        "year",
                        "owner_name",
                        "owner_email",
                    ]
                )
        if not self.asset_path.exists():
            with self.asset_path.open("w", newline="") as fh:
                writer = csv.writer(fh)
                writer.writerow(
                    [
                        "id",
                        "dataset_id",
                        "asset_url",
                        "asset_type",
                        "local_dir",
                        "local_filename",
                        "downloaded_at",
                        "checksum_sha256",
                        "size_bytes",
                        "download_status",
                        "error_message",
                    ]
                )

    def upsert_dataset(self, record: DatasetRecord) -> str:
        dataset_id = record.source_dataset_id or record.source_url
        with self.dataset_path.open("a", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(
                [
                    dataset_id,
                    record.source_name,
                    record.source_dataset_id,
                    record.source_url,
                    record.title,
                    record.description,
                    record.doi,
                    record.license,
                    record.year,
                    record.owner_name,
                    record.owner_email,
                ]
            )
        return dataset_id

    def upsert_asset(self, dataset_id: str, asset: AssetRecord) -> None:
        with self.asset_path.open("a", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(
                [
                    asset.asset_url,
                    dataset_id,
                    asset.asset_url,
                    asset.asset_type,
                    asset.local_dir,
                    asset.local_filename,
                    asset.downloaded_at.isoformat() if asset.downloaded_at else None,
                    asset.checksum_sha256,
                    asset.size_bytes,
                    asset.download_status,
                    asset.error_message,
                ]
            )
