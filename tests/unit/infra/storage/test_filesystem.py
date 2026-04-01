from __future__ import annotations

from pathlib import Path

from qdarchive_seeding.infra.storage.filesystem import FileSystem


def test_filesystem_ensure_dir(tmp_path: Path) -> None:
    fs = FileSystem(root=tmp_path)
    target = tmp_path / "a" / "b"

    result = fs.ensure_dir(target)

    assert result.exists()
    assert result.is_dir()


def test_filesystem_write_text(tmp_path: Path) -> None:
    fs = FileSystem(root=tmp_path)
    target = tmp_path / "nested" / "file.txt"

    fs.write_text(target, "hello")

    assert target.exists()
    assert target.read_text() == "hello"


def test_filesystem_exists(tmp_path: Path) -> None:
    fs = FileSystem(root=tmp_path)
    target = tmp_path / "missing.txt"

    assert fs.exists(target) is False
    target.write_text("x")
    assert fs.exists(target) is True
