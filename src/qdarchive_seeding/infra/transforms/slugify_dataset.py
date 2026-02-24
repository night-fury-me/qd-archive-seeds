from __future__ import annotations

from dataclasses import dataclass
import re

from qdarchive_seeding.core.entities import DatasetRecord
from qdarchive_seeding.infra.transforms.base import BaseTransform


def _slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9\-\s]", "", value)
    value = re.sub(r"\s+", "-", value)
    return value.strip("-")


@dataclass(slots=True)
class SlugifyDataset(BaseTransform):
    def apply(self, record: DatasetRecord) -> DatasetRecord | None:
        if record.title:
            slug = _slugify(record.title)
        elif record.source_dataset_id:
            slug = _slugify(record.source_dataset_id)
        else:
            slug = "dataset"
        if record.raw is None:
            record.raw = {}
        record.raw["dataset_slug"] = slug
        return record
