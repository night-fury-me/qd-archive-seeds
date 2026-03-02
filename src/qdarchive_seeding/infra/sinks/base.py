from __future__ import annotations

from dataclasses import dataclass

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.core.interfaces import Sink


@dataclass(slots=True)
class BaseSink(Sink):
    name: str

    def upsert_dataset(self, record: DatasetRecord) -> str:
        raise NotImplementedError

    def upsert_asset(self, dataset_id: str, asset: AssetRecord) -> None:
        raise NotImplementedError

    def close(self) -> None:
        return None
