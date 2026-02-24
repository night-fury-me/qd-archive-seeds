from __future__ import annotations

from dataclasses import dataclass

from qdarchive_seeding.core.entities import DatasetRecord
from qdarchive_seeding.infra.transforms.base import BaseTransform


@dataclass(slots=True)
class ValidateRequiredFields(BaseTransform):
    required_fields: list[str]

    def apply(self, record: DatasetRecord) -> DatasetRecord | None:
        for field in self.required_fields:
            if not getattr(record, field, None):
                return None
        return record
