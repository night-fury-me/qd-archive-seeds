"""Drop datasets where QDA-relevant files are a small minority."""

from __future__ import annotations

from dataclasses import dataclass

from qdarchive_seeding.core.entities import DatasetRecord
from qdarchive_seeding.infra.transforms.base import BaseTransform


@dataclass(slots=True)
class ExtensionRatioFilter(BaseTransform):
    """Drop datasets where the ratio of primary/analysis files is below a threshold.

    Must run **after** ``ClassifyQdaFiles`` so that each asset already has
    its ``asset_type`` set.

    Parameters
    ----------
    min_primary_ratio : float
        Minimum ratio of (analysis_data + primary_data) files to total
        files.  Defaults to ``0.5`` (majority rule).
    bypass_with_analysis_data : bool
        When ``True`` (default), datasets containing at least one
        analysis-data asset are never dropped.
    """

    min_primary_ratio: float = 0.5
    bypass_with_analysis_data: bool = True

    def apply(self, record: DatasetRecord) -> DatasetRecord | None:  # noqa: D401
        if self.bypass_with_analysis_data and any(
            a.asset_type == "analysis_data" for a in record.assets
        ):
            return record

        if not record.assets:
            return None

        relevant = sum(
            1 for a in record.assets if a.asset_type in ("analysis_data", "primary_data")
        )
        ratio = relevant / len(record.assets)
        if ratio < self.min_primary_ratio:
            return None

        return record
