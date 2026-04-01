from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import PurePosixPath

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.core.interfaces import Transform

logger = logging.getLogger(__name__)


def asset_suffix(asset: AssetRecord) -> str:
    """Get the file extension from local_filename if available, else from the URL."""
    if asset.local_filename:
        return PurePosixPath(asset.local_filename).suffix.lower()
    return PurePosixPath(asset.asset_url).suffix.lower()


@dataclass(slots=True)
class BaseTransform(Transform):
    name: str

    def apply(self, record: DatasetRecord) -> DatasetRecord | None:
        raise NotImplementedError


@dataclass(slots=True)
class TransformChain:
    transforms: list[Transform]

    def run(self, records: Iterable[DatasetRecord]) -> list[DatasetRecord]:
        result: list[DatasetRecord] = []
        for record in records:
            current: DatasetRecord | None = record
            for transform in self.transforms:
                if current is None:
                    break
                current = transform.apply(current)
                if current is None:
                    logger.debug(
                        "Record %s dropped by transform '%s'",
                        record.source_dataset_id or record.title or "unknown",
                        transform.name,
                    )
            if current is not None:
                result.append(current)
        return result
