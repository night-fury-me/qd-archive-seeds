from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd  # type: ignore[import-untyped]
from openpyxl import load_workbook  # type: ignore[import-untyped]

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.infra.sinks.base import FLUSH_INTERVAL, BaseSink


@dataclass(slots=True)
class ExcelSink(BaseSink):
    path: Path
    _dataset_buffer: dict[str, dict[str, object]] = field(default_factory=dict, repr=False)
    _asset_buffer: dict[str, dict[str, object]] = field(default_factory=dict, repr=False)
    _dataset_ops: int = field(default=0, repr=False)
    _asset_ops: int = field(default=0, repr=False)

    def __post_init__(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _sheet_exists(self, sheet_name: str) -> bool:
        if not self.path.exists():
            return False
        wb = load_workbook(self.path, read_only=True)
        exists = sheet_name in wb.sheetnames
        wb.close()
        return exists

    def upsert_dataset(self, record: DatasetRecord) -> str:
        dataset_id = record.source_dataset_id or record.source_url
        self._dataset_buffer[dataset_id] = {
            "id": dataset_id,
            "source_name": record.source_name,
            "source_dataset_id": record.source_dataset_id,
            "source_url": record.source_url,
            "title": record.title,
            "description": record.description,
            "doi": record.doi,
            "license": record.license,
            "year": record.year,
            "owner_name": record.owner_name,
            "owner_email": record.owner_email,
        }
        self._dataset_ops += 1
        if self._dataset_ops >= FLUSH_INTERVAL:
            self._flush_datasets()
        return dataset_id

    def upsert_asset(self, dataset_id: str, asset: AssetRecord) -> None:
        self._asset_buffer[asset.asset_url] = {
            "id": asset.asset_url,
            "dataset_id": dataset_id,
            "asset_url": asset.asset_url,
            "asset_type": asset.asset_type,
            "local_dir": asset.local_dir,
            "local_filename": asset.local_filename,
            "downloaded_at": asset.downloaded_at.isoformat() if asset.downloaded_at else None,
            "checksum_sha256": asset.checksum_sha256,
            "size_bytes": asset.size_bytes,
            "download_status": asset.download_status,
            "error_message": asset.error_message,
        }
        self._asset_ops += 1
        if self._asset_ops >= FLUSH_INTERVAL:
            self._flush_assets()

    def _flush_datasets(self) -> None:
        if not self._dataset_buffer:
            return
        new_df = pd.DataFrame(list(self._dataset_buffer.values()))
        if self._sheet_exists("datasets"):
            existing = pd.read_excel(self.path, sheet_name="datasets")
            # Drop rows that will be replaced by buffer entries
            existing = existing[~existing["id"].isin(self._dataset_buffer)]
            combined = pd.concat([existing, new_df], ignore_index=True)
        else:
            combined = new_df
        self._write_sheet("datasets", combined)
        self._dataset_buffer.clear()
        self._dataset_ops = 0

    def _flush_assets(self) -> None:
        if not self._asset_buffer:
            return
        new_df = pd.DataFrame(list(self._asset_buffer.values()))
        if self._sheet_exists("assets"):
            existing = pd.read_excel(self.path, sheet_name="assets")
            existing = existing[~existing["asset_url"].isin(self._asset_buffer)]
            combined = pd.concat([existing, new_df], ignore_index=True)
        else:
            combined = new_df
        self._write_sheet("assets", combined)
        self._asset_buffer.clear()
        self._asset_ops = 0

    def _write_sheet(self, sheet_name: str, df: pd.DataFrame) -> None:
        """Write a DataFrame to a specific sheet, preserving other sheets."""
        if self.path.exists():
            # Read all existing sheets
            all_sheets: dict[str, pd.DataFrame] = pd.read_excel(self.path, sheet_name=None)
            all_sheets[sheet_name] = df
        else:
            all_sheets = {sheet_name: df}

        with pd.ExcelWriter(self.path) as writer:
            for name, sheet_df in all_sheets.items():
                sheet_df.to_excel(writer, sheet_name=name, index=False)

    def close(self) -> None:
        self._flush_datasets()
        self._flush_assets()
