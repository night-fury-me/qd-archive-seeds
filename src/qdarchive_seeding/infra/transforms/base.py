from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from qdarchive_seeding.core.entities import DatasetRecord
from qdarchive_seeding.core.interfaces import Transform


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
            if current is not None:
                result.append(current)
        return result
