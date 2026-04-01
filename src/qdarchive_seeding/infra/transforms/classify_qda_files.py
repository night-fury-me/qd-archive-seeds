from __future__ import annotations

from dataclasses import dataclass

from qdarchive_seeding.core.entities import DatasetRecord
from qdarchive_seeding.infra.transforms.base import BaseTransform, asset_suffix

ANALYSIS_DATA_EXTENSIONS: frozenset[str] = frozenset(
    {
        # REFI-QDA standard
        ".qdpx",
        ".qdc",
        # MAXQDA (distinctive version-specific formats)
        ".mqda",
        ".mx24",
        ".mx24bac",
        ".mx22",
        ".mx20",
        ".mx18",
        ".mx12",
        ".mx11",
        ".mx5",
        ".mx4",
        ".m2k",
        ".mqbac",
        ".mqtc",
        ".mqex",
        ".mqmtr",
        ".mex24",
        ".mc24",
        ".mex22",
        # NVivo
        ".nvp",
        ".nvpx",
        # ATLAS.ti
        ".atlasproj",
        ".hpr7",
        # Transana
        ".ppj",
        ".pprj",
        # Qualrus
        ".qlt",
        # f4analyse
        ".f4p",
        # QDA Miner
        ".qpd",
    }
)

# Extensions removed from the main set because they cause too many
# false positives with non-QDA software:
#   .mod  — MESA/Fortran model files (astrophysics)
#   .mx3  — MuMax3 micromagnetic simulation scripts
#   .mx2  — ambiguous (very old MAXQDA, also generic)
#   .sea  — macOS Self-Extracting Archive
#   .mtr  — generic metric/measurement files
#   .loa  — generic, rarely used in practice

PRIMARY_DATA_EXTENSIONS: frozenset[str] = frozenset(
    {
        # Documents
        ".pdf",
        ".doc",
        ".docx",
        ".txt",
        ".rtf",
        # Images
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".bmp",
        ".tiff",
        # Audio
        ".mp3",
        ".wav",
        ".m4a",
        ".ogg",
        ".flac",
        # Video
        ".mp4",
        ".avi",
        ".mov",
        ".mkv",
        ".wmv",
        # Tabular
        ".csv",
        ".xlsx",
        ".xls",
        ".tsv",
        ".tab",
        ".ods",
        # Archives (may contain qualitative data)
        ".zip",
    }
)


@dataclass(slots=True)
class ClassifyQdaFiles(BaseTransform):
    def apply(self, record: DatasetRecord) -> DatasetRecord | None:
        for asset in record.assets:
            suffix = asset_suffix(asset)
            if suffix in ANALYSIS_DATA_EXTENSIONS:
                asset.asset_type = "analysis_data"
            elif suffix in PRIMARY_DATA_EXTENSIONS:
                asset.asset_type = "primary_data"
            else:
                asset.asset_type = "additional_data"
        return record
