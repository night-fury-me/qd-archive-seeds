from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.infra.sinks.base import BaseSink


@dataclass(slots=True)
class ExcelSink(BaseSink):
    path: Path

    def __post_init__(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def upsert_dataset(self, record: DatasetRecord) -> str:
        dataset_id = record.source_dataset_id or record.source_url
        df = pd.DataFrame(
            [
                {
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
            ]
        )
        if self.path.exists():
            with pd.ExcelWriter(self.path, mode="a", if_sheet_exists="overlay") as writer:
                df.to_excel(
                    writer,
                    sheet_name="datasets",
                    index=False,
                    header=False,
                    startrow=writer.sheets["datasets"].max_row,
                )
        else:
            with pd.ExcelWriter(self.path) as writer:
                df.to_excel(writer, sheet_name="datasets", index=False)
        return dataset_id

    def upsert_asset(self, dataset_id: str, asset: AssetRecord) -> None:
        df = pd.DataFrame(
            [
                {
                    "id": asset.asset_url,
                    "dataset_id": dataset_id,
                    "asset_url": asset.asset_url,
                    "asset_type": asset.asset_type,
                    "local_dir": asset.local_dir,
                    "local_filename": asset.local_filename,
                    "downloaded_at": asset.downloaded_at.isoformat()
                    if asset.downloaded_at
                    else None,
                    "checksum_sha256": asset.checksum_sha256,
                    "size_bytes": asset.size_bytes,
                    "download_status": asset.download_status,
                    "error_message": asset.error_message,
                }
            ]
        )
        if self.path.exists():
            with pd.ExcelWriter(self.path, mode="a", if_sheet_exists="overlay") as writer:
                df.to_excel(
                    writer,
                    sheet_name="assets",
                    index=False,
                    header=False,
                    startrow=writer.sheets["assets"].max_row,
                )
        else:
            with pd.ExcelWriter(self.path) as writer:
                df.to_excel(writer, sheet_name="assets", index=False)
