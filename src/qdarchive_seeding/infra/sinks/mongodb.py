from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pymongo import MongoClient
from pymongo.database import Database

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.infra.sinks.base import BaseSink


@dataclass(slots=True)
class MongoDBSink(BaseSink):
    uri: str = "mongodb://localhost:27017"
    database: str = "qdarchive"
    _client: MongoClient[dict[str, Any]] | None = field(default=None, init=False, repr=False)
    _db: Database[dict[str, Any]] | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        db = self._ensure_connected()
        db["datasets"].create_index([("source_name", 1), ("source_dataset_id", 1)], unique=True)
        db["assets"].create_index("asset_url", unique=True)

    def _ensure_connected(self) -> Database[dict[str, Any]]:
        if self._client is None:
            self._client = MongoClient(self.uri)
            self._db = self._client[self.database]
        assert self._db is not None  # noqa: S101
        return self._db

    def upsert_dataset(self, record: DatasetRecord) -> str:
        dataset_id = record.source_dataset_id or record.source_url
        db = self._ensure_connected()
        db["datasets"].update_one(
            {"source_name": record.source_name, "source_dataset_id": record.source_dataset_id},
            {
                "$set": {
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
            },
            upsert=True,
        )
        return dataset_id

    def upsert_asset(self, dataset_id: str, asset: AssetRecord) -> None:
        db = self._ensure_connected()
        db["assets"].update_one(
            {"asset_url": asset.asset_url},
            {
                "$set": {
                    "id": asset.asset_url,
                    "dataset_id": dataset_id,
                    "asset_url": asset.asset_url,
                    "asset_type": asset.asset_type,
                    "local_dir": asset.local_dir,
                    "local_filename": asset.local_filename,
                    "downloaded_at": (
                        asset.downloaded_at.isoformat() if asset.downloaded_at else None
                    ),
                    "checksum_sha256": asset.checksum_sha256,
                    "size_bytes": asset.size_bytes,
                    "download_status": asset.download_status,
                    "error_message": asset.error_message,
                }
            },
            upsert=True,
        )

    def get_existing_dataset_ids(self, repository_id: int) -> set[str]:
        return set()

    def get_pending_download_datasets(
        self, repository_id: int | None = None
    ) -> list[tuple[str, DatasetRecord, list[AssetRecord]]]:
        return []

    def get_file_statuses(self, dataset_id: str) -> dict[str, str]:
        return {}

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
            self._db = None
