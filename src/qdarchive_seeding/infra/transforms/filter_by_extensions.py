from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.infra.transforms.base import BaseTransform
from qdarchive_seeding.infra.transforms.classify_qda_files import (
    ANALYSIS_DATA_EXTENSIONS,
    PRIMARY_DATA_EXTENSIONS,
)


@dataclass(slots=True)
class FilterByExtensions(BaseTransform):
    """Keep only assets whose file extension is in the allowed set.

    If *no* assets survive the filter the entire dataset record is dropped
    (returns ``None``).

    Parameters
    ----------
    categories : list[str]
        Which extension categories to keep.  Valid values:
        ``"analysis_data"``, ``"primary_data"``, ``"all"`` (= both).
        Defaults to ``["analysis_data"]``.
    extra_extensions : list[str]
        Additional extensions (with leading dot, e.g. ``".qdp"``) to
        accept regardless of category.
    """

    categories: list[str] = field(default_factory=lambda: ["analysis_data"])
    extra_extensions: list[str] = field(default_factory=list)

    def _allowed_extensions(self) -> frozenset[str]:
        exts: set[str] = set()
        for cat in self.categories:
            if cat in ("analysis_data", "all"):
                exts.update(ANALYSIS_DATA_EXTENSIONS)
            if cat in ("primary_data", "all"):
                exts.update(PRIMARY_DATA_EXTENSIONS)
        exts.update(self.extra_extensions)
        return frozenset(exts)

    @staticmethod
    def _asset_suffix(a: AssetRecord) -> str:
        """Get the file extension from local_filename if available, else from the URL."""
        if a.local_filename:
            return PurePosixPath(a.local_filename).suffix.lower()
        return PurePosixPath(a.asset_url).suffix.lower()

    def apply(self, record: DatasetRecord) -> DatasetRecord | None:
        """Keep the dataset if it contains at least one asset with a matching extension.

        All assets are preserved — the filter only decides whether the
        dataset is relevant, it does not strip individual files.
        """
        allowed = self._allowed_extensions()
        has_match = any(self._asset_suffix(a) in allowed for a in record.assets)
        if not has_match:
            return None
        return record
