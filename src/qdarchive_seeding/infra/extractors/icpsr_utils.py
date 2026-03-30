from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

_CLASSIC_RE = re.compile(r"10\.3886/ICPSR0*(\d+)(?:\.V(\d+))?", re.IGNORECASE)
_OPEN_RE = re.compile(r"10\.3886/E(\d+)(?:V(\d+))?", re.IGNORECASE)

ICPSR_OPEN_HOST = "www.openicpsr.org"
ICPSR_CLASSIC_HOST = "www.icpsr.umich.edu"


@dataclass(slots=True)
class IcpsrDatasetInfo:
    study_id: str
    version: str
    icpsr_type: Literal["classic", "open"]


def parse_icpsr_doi(doi: str) -> IcpsrDatasetInfo | None:
    """Parse an ICPSR DOI to extract study ID and type.

    Classic: doi:10.3886/ICPSR03076.V1 → study_id="3076", type="classic"
    Open:    doi:10.3886/E233925V1     → study_id="233925", type="open"
    Open:    doi:10.3886/E233925       → study_id="233925", type="open", version="V1"
    """
    raw = doi.removeprefix("doi:")

    m = _CLASSIC_RE.search(raw)
    if m:
        return IcpsrDatasetInfo(
            study_id=m.group(1),
            version=f"V{m.group(2)}" if m.group(2) else "V1",
            icpsr_type="classic",
        )

    m = _OPEN_RE.search(raw)
    if m:
        return IcpsrDatasetInfo(
            study_id=m.group(1),
            version=f"V{m.group(2)}" if m.group(2) else "V1",
            icpsr_type="open",
        )

    return None


def build_open_icpsr_download_url(project_id: str, version: str = "V1") -> str:
    """Build the direct download URL for an Open ICPSR project ZIP bundle."""
    return (
        f"https://{ICPSR_OPEN_HOST}/openicpsr/project/{project_id}"
        f"/version/{version}/download/project"
        f"?dirPath=/openicpsr/{project_id}/fcr:versions/{version}"
    )


def build_classic_icpsr_download_url(study_id: str) -> str:
    """Build the zipcart2 download URL for a classic ICPSR dataset.

    Note: This URL requires multi-step form submission (login + terms
    acceptance) — it cannot be downloaded with a simple GET request.
    """
    return (
        f"https://{ICPSR_CLASSIC_HOST}/cgi-bin/bob/zipcart2"
        f"?path=ICPSR&study={study_id}&bundle=all&ds=&dups=yes"
    )
