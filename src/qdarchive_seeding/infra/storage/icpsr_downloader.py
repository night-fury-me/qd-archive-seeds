from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Any

import httpx

from qdarchive_seeding.core.entities import AssetRecord

logger = logging.getLogger(__name__)

_STUDY_RE = re.compile(r"[?&]study=(\d+)")
_PATH_RE = re.compile(r"[?&]path=([^&]+)")

# NOTE: User-agent spoofing may violate ICPSR terms of service.
# Used to bypass download restrictions that block non-browser clients.
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

        # WARNING: ICPSR form fields are hardcoded. If ICPSR adds CSRF tokens
        # or changes form structure, this will silently fail. Consider parsing
        # the terms page HTML to extract actual form fields.

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

        # Step 3: Re-request the download URL (streaming)
        with client.stream("GET", url) as r3:
            content_type = r3.headers.get("content-type", "")
            if "text/html" in content_type:
                logger.error(
                    "ICPSR still returned HTML after terms acceptance for study %s "
                    "(may require additional agreements): %s",
                    study_id,
                    url,
                )
                return None

            return _stream_zip(r3, target_dir, study_id, asset)


def download_open_icpsr(
    asset: AssetRecord,
    target_dir: Path,
    cookies: dict[str, str],
    extra_cookies: dict[str, str] | None = None,
) -> Path | None:
    """Download an Open ICPSR project ZIP with browser cookies.

    Open ICPSR shares SSO with Classic ICPSR, so cookies from both
    domains may be needed.  A Referer header pointing to the project
    page is included because the server may check it.

    Returns the path to the downloaded ZIP, or None on failure.
    """
    url = asset.asset_url
    # Extract project ID from URL path: /openicpsr/project/{id}/...
    project_match = re.search(r"/project/(\d+)/", url)
    project_id = project_match.group(1) if project_match else "unknown"

    target_dir.mkdir(parents=True, exist_ok=True)

    merged_cookies = {**(extra_cookies or {}), **cookies}
    referer = f"https://www.openicpsr.org/openicpsr/project/{project_id}"

    with (
        httpx.Client(
            cookies=merged_cookies,
            headers={"User-Agent": _BROWSER_UA, "Referer": referer},
            follow_redirects=True,
            timeout=120.0,
        ) as client,
        client.stream("GET", url) as r,
    ):
        if r.status_code != 200:
            logger.error(
                "Open ICPSR returned %d for project %s: %s", r.status_code, project_id, url
            )
            return None

        content_type = r.headers.get("content-type", "")
        if "text/html" in content_type:
            logger.error(
                "Open ICPSR returned HTML for project %s "
                "(authentication required or access restricted): %s",
                project_id,
                url,
            )
            return None

        filename = f"openicpsr_{project_id}.zip"
        filepath = target_dir / filename
        part_path = filepath.with_suffix(".part")
        total_bytes = 0
        with part_path.open("wb") as f:
            for chunk in r.iter_bytes(chunk_size=8192):
                f.write(chunk)
                total_bytes += len(chunk)
        os.replace(part_path, filepath)
        asset.local_dir = str(target_dir)
        asset.local_filename = filename
        asset.size_bytes = total_bytes
        logger.info("Downloaded Open ICPSR project %s (%d bytes)", project_id, total_bytes)
        return filepath


def _save_zip(
    content: bytes,
    target_dir: Path,
    study_id: str,
    asset: AssetRecord,
) -> Path:
    """Save ZIP content to disk atomically and update the asset record."""
    filename = f"ICPSR_{study_id.zfill(5)}.zip"
    filepath = target_dir / filename
    part_path = filepath.with_suffix(".part")
    part_path.write_bytes(content)
    os.replace(part_path, filepath)
    asset.local_dir = str(target_dir)
    asset.local_filename = filename
    asset.size_bytes = len(content)
    logger.info("Downloaded Classic ICPSR study %s (%d bytes)", study_id, len(content))
    return filepath


def _stream_zip(
    response: httpx.Response,
    target_dir: Path,
    study_id: str,
    asset: AssetRecord,
) -> Path:
    """Stream ZIP content to disk atomically and update the asset record."""
    filename = f"ICPSR_{study_id.zfill(5)}.zip"
    filepath = target_dir / filename
    part_path = filepath.with_suffix(".part")
    total_bytes = 0
    with part_path.open("wb") as f:
        for chunk in response.iter_bytes(chunk_size=8192):
            f.write(chunk)
            total_bytes += len(chunk)
    os.replace(part_path, filepath)
    asset.local_dir = str(target_dir)
    asset.local_filename = filename
    asset.size_bytes = total_bytes
    logger.info("Downloaded Classic ICPSR study %s (%d bytes)", study_id, total_bytes)
    return filepath
