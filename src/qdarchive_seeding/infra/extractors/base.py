from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from qdarchive_seeding.core.entities import DatasetRecord
from qdarchive_seeding.core.interfaces import Extractor, RunContext


@dataclass(slots=True)
class ExtractorResult:
    records: list[DatasetRecord]
    raw: dict[str, Any] | None = None


@dataclass(slots=True)
class BaseExtractor(Extractor):
    name: str

    def extract(self, ctx: RunContext) -> list[DatasetRecord]:
        raise NotImplementedError
