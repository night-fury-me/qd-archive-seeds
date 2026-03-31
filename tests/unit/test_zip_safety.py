from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pytest

from qdarchive_seeding.app.runner import _validate_zip_members


def _make_zip(members: dict[str, int | None]) -> zipfile.ZipFile:
    """Build an in-memory ZipFile.

    *members* maps filename → file_size override (None = use actual data length).
    For size overrides, we patch the info after writing since writestr sets file_size
    to the actual data length.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, _size_override in members.items():
            zf.writestr(name, b"x" * 16)
    buf.seek(0)
    zf = zipfile.ZipFile(buf, "r")
    # Patch file_size on infos after construction (writestr ignores pre-set sizes)
    for info in zf.infolist():
        override = members.get(info.filename)
        if override is not None:
            info.file_size = override
    return zf


def test_path_traversal_rejected(tmp_path: Path) -> None:
    zf = _make_zip({"../../etc/passwd": None})
    with pytest.raises(ValueError, match="Path traversal detected"):
        _validate_zip_members(zf, tmp_path)
    zf.close()


def test_absolute_path_rejected(tmp_path: Path) -> None:
    zf = _make_zip({"/tmp/evil.txt": None})
    with pytest.raises(ValueError, match="Path traversal detected"):
        _validate_zip_members(zf, tmp_path)
    zf.close()


def test_oversized_file_rejected(tmp_path: Path) -> None:
    # 501 MB declared size
    zf = _make_zip({"big.bin": 501 * 1024 * 1024})
    with pytest.raises(ValueError, match="File too large"):
        _validate_zip_members(zf, tmp_path)
    zf.close()


def test_safe_zip_accepted(tmp_path: Path) -> None:
    zf = _make_zip(
        {
            "data/file1.txt": None,
            "data/file2.csv": None,
        }
    )
    # Should not raise
    _validate_zip_members(zf, tmp_path)
    zf.close()


def test_exactly_500mb_accepted(tmp_path: Path) -> None:
    zf = _make_zip({"exact.bin": 500 * 1024 * 1024})
    # 500 MB is the limit — should pass
    _validate_zip_members(zf, tmp_path)
    zf.close()
