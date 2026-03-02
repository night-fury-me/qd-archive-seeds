from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd  # type: ignore[import-untyped]
from openpyxl import load_workbook  # type: ignore[import-untyped]

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.infra.sinks.base import BaseSink


@dataclass(slots=True)
class ExcelSink(BaseSink):
    path: Path

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
        new_row = {
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
        new_df = pd.DataFrame([new_row])

        if self._sheet_exists("datasets"):
            existing = pd.read_excel(self.path, sheet_name="datasets")
            # Drop existing row with same id, then append new one
            existing = existing[existing["id"] != dataset_id]
            combined = pd.concat([existing, new_df], ignore_index=True)
        else:
            combined = new_df

        self._write_sheet("datasets", combined)
        return dataset_id

    def upsert_asset(self, dataset_id: str, asset: AssetRecord) -> None:
        new_row = {
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
        new_df = pd.DataFrame([new_row])

        if self._sheet_exists("assets"):
            existing = pd.read_excel(self.path, sheet_name="assets")
            existing = existing[existing["asset_url"] != asset.asset_url]
            combined = pd.concat([existing, new_df], ignore_index=True)
        else:
            combined = new_df

        self._write_sheet("assets", combined)

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
