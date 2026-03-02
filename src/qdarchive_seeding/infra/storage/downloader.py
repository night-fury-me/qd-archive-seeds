from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO, cast

import httpx

from qdarchive_seeding.core.constants import (
    DEFAULT_CHUNK_SIZE_BYTES,
    DOWNLOAD_STATUS_FAILED,
    DOWNLOAD_STATUS_RESUMABLE,
    DOWNLOAD_STATUS_SUCCESS,
)
from qdarchive_seeding.core.entities import AssetRecord
from qdarchive_seeding.infra.storage.checksums import ChecksumComputer
from qdarchive_seeding.infra.storage.paths import safe_filename

ProgressCallback = Callable[[int, int | None], None]
"""Called with (bytes_downloaded_so_far, total_bytes_or_none)."""


@dataclass(slots=True)
class DownloadResult:
    asset: AssetRecord
    bytes_downloaded: int
    checksum: str | None


@dataclass(slots=True)
class Downloader:
    client: httpx.Client
    checksum: ChecksumComputer
    chunk_size_bytes: int = DEFAULT_CHUNK_SIZE_BYTES
    on_progress: ProgressCallback | None = field(default=None, repr=False)

    def download(self, asset: AssetRecord, target_dir: Path) -> DownloadResult:
        target_dir.mkdir(parents=True, exist_ok=True)
        filename = safe_filename(asset.local_filename or Path(asset.asset_url).name or "file")
        temp_path = target_dir / f"{filename}.part"
        final_path = target_dir / filename

        headers: dict[str, str] = {}
        mode = "w+b"
        downloaded = 0
        if temp_path.exists():
            downloaded = temp_path.stat().st_size
            headers["Range"] = f"bytes={downloaded}-"
            mode = "a+b"

        try:
            return self._do_stream(asset, headers, mode, temp_path, final_path, target_dir)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 416 and temp_path.exists():
                # Range not satisfiable — stale .part file; restart from scratch
                temp_path.unlink()
                return self._do_stream(asset, {}, "w+b", temp_path, final_path, target_dir)
            asset.download_status = (
                DOWNLOAD_STATUS_RESUMABLE if temp_path.exists() else DOWNLOAD_STATUS_FAILED
            )
            raise
        except Exception:
            asset.download_status = (
                DOWNLOAD_STATUS_RESUMABLE if temp_path.exists() else DOWNLOAD_STATUS_FAILED
            )
            raise

    def _do_stream(
        self,
        asset: AssetRecord,
        headers: dict[str, str],
        mode: str,
        temp_path: Path,
        final_path: Path,
        target_dir: Path,
    ) -> DownloadResult:
        with self.client.stream("GET", asset.asset_url, headers=headers) as response:
            response.raise_for_status()
            content_length = response.headers.get("content-length")
            total_bytes = int(content_length) if content_length else None
            with open(temp_path, mode) as raw_fh:
                fh = cast(BinaryIO, raw_fh)
                checksum = self._stream_to_file(response, fh, asset.asset_url, total_bytes)
        os.replace(temp_path, final_path)
        asset.local_dir = str(target_dir)
        asset.local_filename = final_path.name
        asset.download_status = DOWNLOAD_STATUS_SUCCESS
        asset.downloaded_at = datetime.now(UTC)
        asset.checksum_sha256 = checksum or None
        asset.size_bytes = final_path.stat().st_size
        return DownloadResult(
            asset=asset, bytes_downloaded=asset.size_bytes or 0, checksum=checksum
        )

    def _stream_to_file(
        self,
        response: httpx.Response,
        fh: BinaryIO,
        asset_url: str,
        total_bytes: int | None,
    ) -> str:
        hasher = self.checksum.create_hasher()
        written = 0
        cb = self.on_progress
        for chunk in response.iter_bytes(self.chunk_size_bytes):
            fh.write(chunk)
            hasher.update(chunk)
            written += len(chunk)
            if cb is not None:
                cb(written, total_bytes)
        fh.flush()
        return hasher.hexdigest()
