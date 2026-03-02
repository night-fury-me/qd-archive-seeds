from __future__ import annotations

from dataclasses import dataclass

from qdarchive_seeding.core.entities import DatasetRecord
from qdarchive_seeding.infra.transforms.base import BaseTransform


@dataclass(slots=True)
class ValidateRequiredFields(BaseTransform):
    required_fields: list[str]

    def apply(self, record: DatasetRecord) -> DatasetRecord | None:
        for field in self.required_fields:
            if field == "has_assets":
                if not record.assets:
                    return None
            elif field.startswith("assets."):
                # Check a field on every asset, e.g. "assets.asset_url"
                attr = field.split(".", 1)[1]
                if not record.assets or not all(getattr(a, attr, None) for a in record.assets):
                    return None
            else:
                if not getattr(record, field, None):
                    return None
        return record
