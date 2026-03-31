from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
from urllib.parse import urlparse

from qdarchive_seeding.core.entities import DatasetRecord
from qdarchive_seeding.infra.transforms.base import BaseTransform


@dataclass(slots=True)
class InferFileTypes(BaseTransform):
    def apply(self, record: DatasetRecord) -> DatasetRecord | None:
        for asset in record.assets:
            if asset.asset_type:
                continue
            suffix = PurePosixPath(urlparse(asset.asset_url).path).suffix.lower()
            if suffix in {".qdpx", ".qdp"}:
                asset.asset_type = suffix.lstrip(".")
            elif suffix in {".zip", ".tar", ".gz", ".tgz"}:
                asset.asset_type = "archive"
            elif suffix in {".pdf", ".docx", ".doc"}:
                asset.asset_type = "document"
            else:
                asset.asset_type = "unknown"
        return record
