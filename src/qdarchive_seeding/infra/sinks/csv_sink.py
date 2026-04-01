from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from pathlib import Path

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.infra.sinks._buffered import BufferedSink

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
    "query_string",
    "repository_id",
    "repository_url",
    "version",
    "language",
    "upload_date",
    "download_date",
    "download_repository_folder",
    "download_project_folder",
    "download_version_folder",
    "download_method",
    "is_harvested",
    "harvested_from",
    "keywords",
    "persons",
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
    """Write headers and rows to a CSV file atomically via tmp+replace."""
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(headers)
        for row in rows_dict.values():
            writer.writerow(row)
    os.replace(tmp, path)


@dataclass(slots=True)
class CSVSink(BufferedSink):
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
        keywords_str = ",".join(record.keywords) if record.keywords else ""
        persons_str = (
            ",".join(f"{p.name}:{p.role}" for p in record.persons) if record.persons else ""
        )
        row = [
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
            record.query_string or "",
            str(record.repository_id) if record.repository_id is not None else "",
            record.repository_url or "",
            record.version or "",
            record.language or "",
            record.upload_date or "",
            record.download_date or "",
            record.download_repository_folder or "",
            record.download_project_folder or "",
            record.download_version_folder or "",
            record.download_method or "",
            str(record.is_harvested),
            record.harvested_from or "",
            keywords_str,
            persons_str,
        ]
        return self._buffer_dataset(dataset_id, row)

    def upsert_asset(self, dataset_id: str, asset: AssetRecord) -> None:
        row = [
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
        self._buffer_asset(asset.asset_url, row)

    def _flush_datasets(self) -> None:
        if not self._dataset_buffer:
            return
        _, existing = _read_csv(self.dataset_path, "id")
        existing.update(self._dataset_buffer)
        _write_csv(self.dataset_path, DATASET_HEADERS, existing)
        self._dataset_buffer.clear()
        self._dataset_ops = 0

    def _flush_assets(self) -> None:
        if not self._asset_buffer:
            return
        _, existing = _read_csv(self.asset_path, "asset_url")
        existing.update(self._asset_buffer)
        _write_csv(self.asset_path, ASSET_HEADERS, existing)
        self._asset_buffer.clear()
        self._asset_ops = 0

    def get_existing_dataset_ids(self, repository_id: int) -> set[str]:
        return set()

    def get_pending_download_datasets(
        self, repository_id: int | None = None
    ) -> list[tuple[str, DatasetRecord, list[AssetRecord]]]:
        return []

    def get_file_statuses(self, dataset_id: str) -> dict[str, str]:
        return {}
