from __future__ import annotations

from dataclasses import dataclass

from pymongo import MongoClient

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.infra.sinks.base import BaseSink


@dataclass(slots=True)
class MongoDBSink(BaseSink):
    uri: str = "mongodb://localhost:27017"
    database: str = "qdarchive"

    def __post_init__(self) -> None:
        client: MongoClient[dict[str, object]] = MongoClient(self.uri)
        db = client[self.database]
        db["datasets"].create_index(
            [("source_name", 1), ("source_dataset_id", 1)], unique=True
        )
        db["assets"].create_index("asset_url", unique=True)
        client.close()

    def _get_client(self) -> MongoClient[dict[str, object]]:
        return MongoClient(self.uri)

    def upsert_dataset(self, record: DatasetRecord) -> str:
        dataset_id = record.source_dataset_id or record.source_url
        client = self._get_client()
        try:
            db = client[self.database]
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
        finally:
            client.close()
        return dataset_id

    def upsert_asset(self, dataset_id: str, asset: AssetRecord) -> None:
        client = self._get_client()
        try:
            db = client[self.database]
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
        finally:
            client.close()
