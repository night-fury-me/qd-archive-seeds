from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.core.interfaces import RunContext


@dataclass(slots=True)
class StaticListOptions:
    records: list[dict[str, Any]]


@dataclass(slots=True)
class StaticListExtractor:
    options: StaticListOptions

    async def extract(self, ctx: RunContext) -> AsyncIterator[DatasetRecord]:
        for item in self.options.records:
            yield DatasetRecord(
                source_name=ctx.config.source.name,
                source_dataset_id=str(item.get("id")) if item.get("id") is not None else None,
                source_url=str(item.get("source_url") or ""),
                title=item.get("title"),
                description=item.get("description"),
                assets=[
                    AssetRecord(asset_url=str(asset))
                    for asset in item.get("assets", [])
                    if asset is not None
                ],
                raw=item,
            )
