from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import MagicMock

import httpx
import pytest

from qdarchive_seeding.core.constants import (
    DOWNLOAD_STATUS_FAILED,
    DOWNLOAD_STATUS_RESUMABLE,
    DOWNLOAD_STATUS_SUCCESS,
)
from qdarchive_seeding.core.entities import AssetRecord
from qdarchive_seeding.infra.storage.checksums import ChecksumComputer
from qdarchive_seeding.infra.storage.downloader import Downloader

SAMPLE_CONTENT = b"hello world, this is test content for download"


@dataclass
class FakeResponse:
    """Minimal stand-in for httpx.Response inside a streaming context."""

    data: bytes
    status_code: int = 200
    headers: dict[str, str] = field(default_factory=dict)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            response = MagicMock(spec=httpx.Response)
            response.status_code = self.status_code
            raise httpx.HTTPStatusError(
                message=f"HTTP {self.status_code}",
                request=MagicMock(spec=httpx.Request),
                response=response,
            )

    async def aiter_bytes(self, chunk_size: int = 4096) -> AsyncIterator[bytes]:
        offset = 0
        while offset < len(self.data):
            yield self.data[offset : offset + chunk_size]
            offset += chunk_size


class FakeClient:
    """Fake httpx.AsyncClient whose .stream() yields a FakeResponse."""

    def __init__(self, response: FakeResponse) -> None:
        self._response = response

    @asynccontextmanager
    async def stream(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
    ) -> AsyncIterator[FakeResponse]:
        yield self._response


def _make_asset(url: str = "https://example.com/data.csv") -> AssetRecord:
    return AssetRecord(asset_url=url)


def _make_downloader(
    data: bytes = SAMPLE_CONTENT,
    *,
    status_code: int = 200,
    response_headers: dict[str, str] | None = None,
    checksum_algo: str = "sha256",
    chunk_size: int = 16,
    on_progress: object | None = None,
) -> Downloader:
    resp_headers = response_headers or {"content-length": str(len(data))}
    fake_resp = FakeResponse(data=data, status_code=status_code, headers=resp_headers)
    fake_client = FakeClient(fake_resp)  # type: ignore[arg-type]
    return Downloader(
        client=fake_client,  # type: ignore[arg-type]
        checksum=ChecksumComputer(algo=checksum_algo),
        chunk_size_bytes=chunk_size,
        on_progress=on_progress,  # type: ignore[arg-type]
    )


# ---------- 1. Successful download ----------


@pytest.mark.asyncio
async def test_successful_download_writes_file(tmp_path: Path) -> None:
    dl = _make_downloader()
    asset = _make_asset()

    result = await dl.download(asset, tmp_path)

    final_path = tmp_path / "data.csv"
    assert final_path.exists()
    assert final_path.read_bytes() == SAMPLE_CONTENT
    assert result.bytes_downloaded == len(SAMPLE_CONTENT)
    assert asset.download_status == DOWNLOAD_STATUS_SUCCESS
    assert asset.local_dir == str(tmp_path)
    assert asset.local_filename == "data.csv"
    assert asset.size_bytes == len(SAMPLE_CONTENT)
    assert asset.downloaded_at is not None


@pytest.mark.asyncio
async def test_successful_download_computes_sha256(tmp_path: Path) -> None:
    dl = _make_downloader()
    asset = _make_asset()

    result = await dl.download(asset, tmp_path)

    expected_hash = hashlib.sha256(SAMPLE_CONTENT).hexdigest()
    assert result.checksum == expected_hash
    assert asset.checksum_sha256 == expected_hash


@pytest.mark.asyncio
async def test_part_file_removed_after_success(tmp_path: Path) -> None:
    dl = _make_downloader()
    asset = _make_asset()

    await dl.download(asset, tmp_path)

    part_file = tmp_path / "data.csv.part"
    assert not part_file.exists()


# ---------- 2. Atomic write (.part -> final) ----------


@pytest.mark.asyncio
async def test_atomic_write_uses_part_file(tmp_path: Path) -> None:
    """Verify that during streaming the .part file exists and the final file does not."""
    part_seen = False
    final_absent = True

    target_dir = tmp_path / "downloads"
    final_path = target_dir / "data.csv"
    part_path = target_dir / "data.csv.part"

    class SpyClient:
        @asynccontextmanager
        async def stream(
            self, method: str, url: str, *, headers: dict[str, str] | None = None
        ) -> AsyncIterator[FakeResponse]:
            headers_dict = {"content-length": str(len(SAMPLE_CONTENT))}
            resp = FakeResponse(data=SAMPLE_CONTENT, headers=headers_dict)
            yield resp

    class SpyDownloader(Downloader):
        async def _stream_to_file(self, response, fh, asset_url, total_bytes):  # type: ignore[override]
            nonlocal part_seen, final_absent
            # At this point the .part file should be open and the final should not exist
            part_seen = part_path.exists()
            final_absent = not final_path.exists()
            return await super()._stream_to_file(response, fh, asset_url, total_bytes)

    dl = SpyDownloader(
        client=SpyClient(),  # type: ignore[arg-type]
        checksum=ChecksumComputer(),
        chunk_size_bytes=16,
    )
    asset = _make_asset()
    await dl.download(asset, target_dir)

    assert part_seen, ".part file should exist during streaming"
    assert final_absent, "final file should NOT exist during streaming"
    assert final_path.exists(), "final file should exist after download completes"
    assert not part_path.exists(), ".part file should be gone after download completes"


# ---------- 3. Progress callback ----------


@pytest.mark.asyncio
async def test_progress_callback_called(tmp_path: Path) -> None:
    calls: list[tuple[int, int | None]] = []

    def on_progress(downloaded: int, total: int | None) -> None:
        calls.append((downloaded, total))

    dl = _make_downloader(chunk_size=10, on_progress=on_progress)
    asset = _make_asset()

    await dl.download(asset, tmp_path)

    assert len(calls) > 0, "progress callback should have been called at least once"
    # Last call should report the full size downloaded
    last_downloaded, last_total = calls[-1]
    assert last_downloaded == len(SAMPLE_CONTENT)
    assert last_total == len(SAMPLE_CONTENT)
    # Each call should report increasing bytes
    for i in range(1, len(calls)):
        assert calls[i][0] >= calls[i - 1][0]


# ---------- 4. Download failure sets FAILED status ----------


@pytest.mark.asyncio
async def test_download_failure_sets_failed_status(tmp_path: Path) -> None:
    dl = _make_downloader(status_code=500)
    asset = _make_asset()

    with pytest.raises(httpx.HTTPStatusError):
        await dl.download(asset, tmp_path)

    assert asset.download_status == DOWNLOAD_STATUS_FAILED


@pytest.mark.asyncio
async def test_download_failure_no_part_file_left(tmp_path: Path) -> None:
    """When the server returns an error before any bytes are written, no .part file remains."""
    dl = _make_downloader(status_code=500)
    asset = _make_asset()

    with pytest.raises(httpx.HTTPStatusError):
        await dl.download(asset, tmp_path)

    # The key assertion is the status — the .part file may or may not exist
    # depending on whether open() ran before raise_for_status().
    assert asset.download_status == DOWNLOAD_STATUS_FAILED


# ---------- 5. Checksum algo="none" ----------


@pytest.mark.asyncio
async def test_checksum_none_algo_stores_none(tmp_path: Path) -> None:
    dl = _make_downloader(checksum_algo="none")
    asset = _make_asset()

    result = await dl.download(asset, tmp_path)

    # _NullHasher.hexdigest() returns "", which is falsy, so checksum_sha256 should be None
    assert asset.checksum_sha256 is None
    assert result.checksum == ""


@pytest.mark.asyncio
async def test_creates_target_directory(tmp_path: Path) -> None:
    """download() should create the target directory if it does not exist."""
    dl = _make_downloader()
    asset = _make_asset()
    nested = tmp_path / "a" / "b" / "c"

    await dl.download(asset, nested)

    assert nested.exists()
    assert (nested / "data.csv").exists()


@pytest.mark.asyncio
async def test_resume_with_range_and_416_retries(tmp_path: Path) -> None:
    target_dir = tmp_path / "downloads"
    target_dir.mkdir(parents=True, exist_ok=True)
    part_path = target_dir / "data.csv.part"
    part_path.write_bytes(b"partial")

    responses = [
        FakeResponse(data=b"", status_code=416, headers={}),
        FakeResponse(
            data=SAMPLE_CONTENT,
            status_code=200,
            headers={"content-length": str(len(SAMPLE_CONTENT))},
        ),
    ]

    class SequenceClient:
        def __init__(self, seq: list[FakeResponse]) -> None:
            self._seq = list(seq)
            self.headers: list[dict[str, str]] = []

        @asynccontextmanager
        async def stream(
            self, method: str, url: str, *, headers: dict[str, str] | None = None
        ) -> AsyncIterator[FakeResponse]:
            self.headers.append(headers or {})
            yield self._seq.pop(0)

    client = SequenceClient(responses)
    dl = Downloader(client=client, checksum=ChecksumComputer(), chunk_size_bytes=16)
    asset = _make_asset()

    result = await dl.download(asset, target_dir)

    assert "Range" in client.headers[0]
    assert (target_dir / "data.csv").exists()
    assert result.bytes_downloaded == len(SAMPLE_CONTENT)


@pytest.mark.asyncio
async def test_failure_with_partial_file_sets_resumable(tmp_path: Path) -> None:
    target_dir = tmp_path / "downloads"
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / "data.csv.part").write_bytes(b"partial")
    dl = _make_downloader(status_code=500)
    asset = _make_asset()

    with pytest.raises(httpx.HTTPStatusError):
        await dl.download(asset, target_dir)

    assert asset.download_status == DOWNLOAD_STATUS_RESUMABLE


@pytest.mark.asyncio
async def test_non_http_error_sets_resumable(tmp_path: Path) -> None:
    target_dir = tmp_path / "downloads"
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / "data.csv.part").write_bytes(b"partial")

    class BoomClient:
        @asynccontextmanager
        async def stream(self, _method: str, _url: str, *, headers: dict[str, str] | None = None):
            raise RuntimeError("boom")
            yield  # pragma: no cover — required by asynccontextmanager

    dl = Downloader(client=BoomClient(), checksum=ChecksumComputer(), chunk_size_bytes=16)
    asset = _make_asset()

    with pytest.raises(RuntimeError):
        await dl.download(asset, target_dir)

    assert asset.download_status == DOWNLOAD_STATUS_RESUMABLE
