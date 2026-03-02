from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.infra.sinks.base import BaseSink

DATASET_HEADERS = [
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

ASSET_HEADERS = [
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


def _read_csv(path: Path, key_column: str) -> tuple[list[str], dict[str, list[str]]]:
    """Read a CSV file and return (headers, rows_dict keyed by key_column)."""
    rows: dict[str, list[str]] = {}
    headers: list[str] = []
    if not path.exists():
        return headers, rows
    with path.open("r", newline="") as fh:
        reader = csv.reader(fh)
        headers = next(reader, [])
        if not headers:
            return headers, rows
        key_idx = headers.index(key_column) if key_column in headers else 0
        for row in reader:
            if row:
                rows[row[key_idx]] = row
    return headers, rows


def _write_csv(path: Path, headers: list[str], rows_dict: dict[str, list[str]]) -> None:
    """Write headers and rows to a CSV file."""
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(headers)
        for row in rows_dict.values():
            writer.writerow(row)


@dataclass(slots=True)
class CSVSink(BaseSink):
    dataset_path: Path
    asset_path: Path

    def __post_init__(self) -> None:
        self.dataset_path.parent.mkdir(parents=True, exist_ok=True)
        self.asset_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.dataset_path.exists():
            _write_csv(self.dataset_path, DATASET_HEADERS, {})
        if not self.asset_path.exists():
            _write_csv(self.asset_path, ASSET_HEADERS, {})

    def upsert_dataset(self, record: DatasetRecord) -> str:
        dataset_id = record.source_dataset_id or record.source_url
        _, rows = _read_csv(self.dataset_path, "id")
        rows[dataset_id] = [
            dataset_id,
            record.source_name,
            record.source_dataset_id or "",
            record.source_url,
            record.title or "",
            record.description or "",
            record.doi or "",
            record.license or "",
            str(record.year) if record.year is not None else "",
            record.owner_name or "",
            record.owner_email or "",
        ]
        _write_csv(self.dataset_path, DATASET_HEADERS, rows)
        return dataset_id

    def upsert_asset(self, dataset_id: str, asset: AssetRecord) -> None:
        _, rows = _read_csv(self.asset_path, "asset_url")
        rows[asset.asset_url] = [
            asset.asset_url,
            dataset_id,
            asset.asset_url,
            asset.asset_type or "",
            asset.local_dir or "",
            asset.local_filename or "",
            asset.downloaded_at.isoformat() if asset.downloaded_at else "",
            asset.checksum_sha256 or "",
            str(asset.size_bytes) if asset.size_bytes is not None else "",
            asset.download_status or "",
            asset.error_message or "",
        ]
        _write_csv(self.asset_path, ASSET_HEADERS, rows)
