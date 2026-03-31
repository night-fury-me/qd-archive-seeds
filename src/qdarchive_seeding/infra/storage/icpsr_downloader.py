from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import httpx

from qdarchive_seeding.core.entities import AssetRecord

logger = logging.getLogger(__name__)

_STUDY_RE = re.compile(r"[?&]study=(\d+)")
_PATH_RE = re.compile(r"[?&]path=([^&]+)")

_BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


def download_classic_icpsr(
    asset: AssetRecord,
    target_dir: Path,
    cookies: dict[str, str],
) -> Path | None:
    """Download a Classic ICPSR dataset via the 3-step zipcart2 flow.

    1. GET zipcart2 URL → Terms of Use page
    2. POST /cgi-bin/terms (accept) → "Download shortly" page
    3. GET zipcart2 URL again → actual ZIP file

    Returns the path to the downloaded ZIP, or None on failure.
    """
    url = asset.asset_url
    study_match = _STUDY_RE.search(url)
    path_match = _PATH_RE.search(url)
    if not study_match:
        logger.error("Cannot parse study ID from URL: %s", url)
        return None

    study_id = study_match.group(1)
    icpsr_path = path_match.group(1) if path_match else "ICPSR"

    target_dir.mkdir(parents=True, exist_ok=True)

    with httpx.Client(
        cookies=cookies,
        headers={"User-Agent": _BROWSER_UA},
        follow_redirects=True,
        timeout=120.0,
    ) as client:
        # Step 1: GET terms page
        r1 = client.get(url)
        if r1.status_code != 200:
            logger.error("ICPSR terms page returned %d for study %s", r1.status_code, study_id)
            return None

        # If we got the ZIP directly (terms already accepted in session), save it
        if "text/html" not in r1.headers.get("content-type", ""):
            return _save_zip(r1.content, target_dir, study_id, asset)

        # Step 2: Accept terms
        form_data: dict[str, Any] = {
            "agree": "yes",
            "path": icpsr_path,
            "study": study_id,
            "ds": "",
            "bundle": "all",
            "dups": "yes",
            ".submit": "I Agree",
        }
        r2 = client.post("https://www.icpsr.umich.edu/cgi-bin/terms", data=form_data)
        if r2.status_code != 200:
            logger.error("ICPSR terms POST returned %d for study %s", r2.status_code, study_id)
            return None

        # Step 3: Re-request the download URL
        r3 = client.get(url)
        content_type = r3.headers.get("content-type", "")
        if "text/html" in content_type:
            logger.error(
                "ICPSR still returned HTML after terms acceptance for study %s "
                "(may require additional agreements): %s",
                study_id,
                url,
            )
            return None

        return _save_zip(r3.content, target_dir, study_id, asset)


def _save_zip(
    content: bytes,
    target_dir: Path,
    study_id: str,
    asset: AssetRecord,
) -> Path:
    """Save ZIP content to disk and update the asset record."""
    filename = f"ICPSR_{study_id.zfill(5)}.zip"
    filepath = target_dir / filename
    filepath.write_bytes(content)
    asset.local_dir = str(target_dir)
    asset.local_filename = filename
    asset.size_bytes = len(content)
    logger.info("Downloaded Classic ICPSR study %s (%d bytes)", study_id, len(content))
    return filepath
