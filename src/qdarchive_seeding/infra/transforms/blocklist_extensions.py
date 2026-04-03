"""Drop datasets that contain file extensions associated with non-QDA scientific domains."""

from __future__ import annotations

from dataclasses import dataclass, field

from qdarchive_seeding.core.entities import DatasetRecord
from qdarchive_seeding.infra.transforms.base import BaseTransform, asset_suffix
from qdarchive_seeding.infra.transforms.classify_qda_files import (
    ANALYSIS_DATA_EXTENSIONS,
)

# ---------------------------------------------------------------------------
# Science-specific extensions that are never found in qualitative research
# datasets.  A single match is enough to classify a dataset as non-QDA.
# ---------------------------------------------------------------------------
SCIENCE_BLOCKLIST_EXTENSIONS: frozenset[str] = frozenset(
    {
        # Physics / Astrophysics
        ".fits",
        ".fts",
        ".fit",
        ".hdf5",
        ".h5",
        ".hdf",
        ".nc",
        ".nc4",
        ".cdf",
        ".nex",
        ".nxs",
        # Chemistry / Molecular
        ".pdb",
        ".mol",
        ".mol2",
        ".sdf",
        ".cif",
        ".mmcif",
        ".xyz",
        ".cube",
        # Biology / Genomics
        ".fasta",
        ".fa",
        ".fq",
        ".fastq",
        ".bam",
        ".sam",
        ".cram",
        ".vcf",
        ".bcf",
        ".bed",
        ".gff",
        ".gtf",
        ".nwk",
        ".newick",
        # Geospatial / GIS
        ".shp",
        ".shx",
        ".dbf",
        ".gpkg",
        ".geojson",
        ".kml",
        ".kmz",
        ".las",
        ".laz",
        ".dem",
        # Engineering / Simulation
        ".stl",
        ".vtk",
        ".vtu",
        ".vtp",
        ".msh",
        ".inp",
        # Numeric / ML serialisation
        ".npy",
        ".npz",
        ".mat",
        ".parquet",
        ".feather",
        ".arrow",
        ".pkl",
        ".pickle",
        ".dill",
        # Microscopy
        ".nd2",
        ".czi",
        ".svs",
        ".dm3",
        ".dm4",
        # Spectroscopy / Mass-spec
        ".mzml",
        ".mzxml",
        ".spc",
        # Other domain-specific formats
        ".rodhypix",
        ".pae",
        ".dssp",
        ".swc",
        ".glb",
        ".conllu",
    }
)


@dataclass(slots=True)
class BlocklistExtensions(BaseTransform):
    """Drop datasets containing extensions associated with non-QDA scientific domains.

    If *any* asset has a blocked extension the entire dataset is dropped,
    **unless** the dataset also contains a recognised QDA analysis file
    (controlled by ``bypass_with_analysis_data``).

    Parameters
    ----------
    blocked_extensions : frozenset[str]
        Extensions (with leading dot) to treat as non-QDA signals.
    bypass_with_analysis_data : bool
        When ``True`` (default), datasets that contain at least one
        analysis-data extension are never dropped.
    """

    blocked_extensions: frozenset[str] = field(default_factory=lambda: SCIENCE_BLOCKLIST_EXTENSIONS)
    bypass_with_analysis_data: bool = True

    def apply(self, record: DatasetRecord) -> DatasetRecord | None:  # noqa: D401
        if self.bypass_with_analysis_data and any(
            asset_suffix(a) in ANALYSIS_DATA_EXTENSIONS for a in record.assets
        ):
            return record

        if any(asset_suffix(a) in self.blocked_extensions for a in record.assets):
            return None

        return record
