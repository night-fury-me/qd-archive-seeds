from __future__ import annotations

import contextlib
import logging
import os
import zipfile
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

from qdarchive_seeding.core.constants import DOWNLOAD_STATUS_SUCCESS
from qdarchive_seeding.core.entities import AssetRecord


def validate_zip_members(zf: zipfile.ZipFile, target_dir: Path) -> None:
    """Reject ZIPs containing path-traversal entries or oversized files."""
    resolved_target = target_dir.resolve()
    for member in zf.infolist():
        member_path = (target_dir / member.filename).resolve()
        if (
            not str(member_path).startswith(str(resolved_target) + os.sep)
            and member_path != resolved_target
        ):
            raise ValueError(f"Path traversal detected: {member.filename}")
        if member.file_size > 500 * 1024 * 1024:  # 500 MB per file
            raise ValueError(f"File too large: {member.filename} ({member.file_size} bytes)")


def extract_zip_bundle(
    zip_path: Path,
    target_dir: Path,
    log: logging.Logger,
) -> list[Any]:
    """Extract a ZIP bundle and return AssetRecords for individual files."""
    if not zip_path.exists() or not zipfile.is_zipfile(zip_path):
        log.warning("Not a valid ZIP file: %s", zip_path)
        return []

    extracted: list[AssetRecord] = []
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            validate_zip_members(zf, target_dir)
            zf.extractall(target_dir)

            # Detect single top-level directory and flatten it
            top_level_dirs = {
                PurePosixPath(info.filename).parts[0]
                for info in zf.infolist()
                if len(PurePosixPath(info.filename).parts) > 1
            }
            single_subdir = (
                (target_dir / next(iter(top_level_dirs))) if len(top_level_dirs) == 1 else None
            )
            if single_subdir and single_subdir.is_dir():
                import shutil

                for child in list(single_subdir.iterdir()):
                    dest = target_dir / child.name
                    if dest.exists():
                        # Merge: if both are dirs, move contents; otherwise skip
                        if child.is_dir() and dest.is_dir():
                            for sub in child.rglob("*"):
                                if sub.is_file():
                                    rel = sub.relative_to(child)
                                    sub_dest = dest / rel
                                    sub_dest.parent.mkdir(parents=True, exist_ok=True)
                                    if not sub_dest.exists():
                                        shutil.move(str(sub), str(sub_dest))
                            shutil.rmtree(child)
                        # else: destination file already exists, skip
                    else:
                        shutil.move(str(child), str(dest))
                # Remove subdir if now empty
                with contextlib.suppress(OSError):
                    single_subdir.rmdir()
                log.debug("Flattened single top-level directory: %s", single_subdir.name)

            for info in zf.infolist():
                if info.is_dir():
                    continue
                # Strip the top-level directory prefix if we flattened
                filename = info.filename
                if single_subdir is not None:
                    parts = PurePosixPath(filename).parts
                    filename = str(PurePosixPath(*parts[1:])) if len(parts) > 1 else filename
                file_path = target_dir / filename
                file_type = PurePosixPath(filename).suffix.lstrip(".") or None
                extracted.append(
                    AssetRecord(
                        asset_url=zip_path.name,
                        local_dir=str(file_path.parent),
                        local_filename=file_path.name,
                        file_type=file_type,
                        size_bytes=info.file_size,
                        download_status=DOWNLOAD_STATUS_SUCCESS,
                        downloaded_at=datetime.now(UTC),
                    )
                )
        # Remove the ZIP after successful extraction
        zip_path.unlink()
        log.info("Extracted %d files from %s", len(extracted), zip_path.name)
    except Exception as exc:
        log.error("Failed to extract ZIP %s: %s", zip_path, exc)
        return []
    return extracted
