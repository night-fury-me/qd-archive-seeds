from __future__ import annotations

from dataclasses import dataclass

from qdarchive_seeding.core.entities import DatasetRecord
from qdarchive_seeding.infra.transforms.base import BaseTransform


@dataclass(slots=True)
class DeduplicateAssets(BaseTransform):
    def apply(self, record: DatasetRecord) -> DatasetRecord | None:
        seen: set[str] = set()
        unique_assets = []
        for asset in record.assets:
            if asset.asset_url in seen:
                continue
            seen.add(asset.asset_url)
            unique_assets.append(asset)
        record.assets = unique_assets
        return record
