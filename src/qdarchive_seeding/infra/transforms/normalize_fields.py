from __future__ import annotations

from dataclasses import dataclass

from qdarchive_seeding.core.entities import DatasetRecord
from qdarchive_seeding.infra.transforms.base import BaseTransform


@dataclass(slots=True)
class NormalizeFields(BaseTransform):
    def apply(self, record: DatasetRecord) -> DatasetRecord | None:
        if record.owner_name is None and record.raw:
            owner = (
                record.raw.get("owner") or record.raw.get("uploader") or record.raw.get("author")
            )
            if isinstance(owner, dict):
                record.owner_name = owner.get("name") or owner.get("full_name")
                record.owner_email = owner.get("email") or owner.get("mail")
            elif isinstance(owner, str):
                record.owner_name = owner

        if record.title is None and record.raw:
            record.title = record.raw.get("name") or record.raw.get("title")
        if record.description is None and record.raw:
            record.description = record.raw.get("summary") or record.raw.get("description")
        return record
