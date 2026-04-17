from __future__ import annotations

import sqlite3
from pathlib import Path

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord, PersonRole
from qdarchive_seeding.infra.sinks.sqlite import SQLiteSink


def _make_record(**kwargs: object) -> DatasetRecord:
    defaults: dict[str, object] = {
        "source_name": "zenodo",
        "source_dataset_id": "12345",
        "source_url": "https://zenodo.org/records/12345",
        "title": "Test Dataset",
        "repository_id": 1,
        "repository_url": "https://zenodo.org",
        "download_project_folder": "12345",
        "download_repository_folder": "zenodo",
        "download_method": "API-CALL",
    }
    defaults.update(kwargs)
    return DatasetRecord(**defaults)  # type: ignore[arg-type]


def test_upsert_dataset_creates_project(tmp_path: Path) -> None:
    sink = SQLiteSink(name="test", path=tmp_path / "test.sqlite")
    record = _make_record()
    pid = sink.upsert_dataset(record)
    sink.close()

    conn = sqlite3.connect(tmp_path / "test.sqlite")
    count = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    conn.close()
    assert count == 1
    assert pid == "1"


def test_upsert_dataset_idempotent(tmp_path: Path) -> None:
    sink = SQLiteSink(name="test", path=tmp_path / "test.sqlite")
    record = _make_record()
    pid1 = sink.upsert_dataset(record)
    pid2 = sink.upsert_dataset(record)
    sink.close()

    conn = sqlite3.connect(tmp_path / "test.sqlite")
    count = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    conn.close()
    assert count == 1
    assert pid1 == pid2


def test_upsert_dataset_stores_keywords(tmp_path: Path) -> None:
    sink = SQLiteSink(name="test", path=tmp_path / "test.sqlite")
    record = _make_record(keywords=["interview", "qualitative"])
    pid = sink.upsert_dataset(record)
    sink.close()

    conn = sqlite3.connect(tmp_path / "test.sqlite")
    rows = conn.execute(
        "SELECT keyword FROM keywords WHERE project_id = ? ORDER BY keyword",
        (int(pid),),
    ).fetchall()
    conn.close()
    assert [r[0] for r in rows] == ["interview", "qualitative"]


def test_upsert_dataset_stores_persons(tmp_path: Path) -> None:
    sink = SQLiteSink(name="test", path=tmp_path / "test.sqlite")
    record = _make_record(
        persons=[
            PersonRole(name="Alice", role="AUTHOR"),
            PersonRole(name="Bob", role="OTHER"),
        ]
    )
    pid = sink.upsert_dataset(record)
    sink.close()

    conn = sqlite3.connect(tmp_path / "test.sqlite")
    rows = conn.execute(
        "SELECT name, role FROM person_role WHERE project_id = ? ORDER BY name",
        (int(pid),),
    ).fetchall()
    conn.close()
    assert rows == [("Alice", "AUTHOR"), ("Bob", "OTHER")]


def test_upsert_dataset_stores_license(tmp_path: Path) -> None:
    sink = SQLiteSink(name="test", path=tmp_path / "test.sqlite")
    record = _make_record(license="CC BY 4.0")
    pid = sink.upsert_dataset(record)
    sink.close()

    conn = sqlite3.connect(tmp_path / "test.sqlite")
    row = conn.execute("SELECT license FROM licenses WHERE project_id = ?", (int(pid),)).fetchone()
    conn.close()
    assert row[0] == "CC BY 4.0"


def test_upsert_asset_creates_file(tmp_path: Path) -> None:
    sink = SQLiteSink(name="test", path=tmp_path / "test.sqlite")
    record = _make_record()
    pid = sink.upsert_dataset(record)
    asset = AssetRecord(
        asset_url="https://zenodo.org/files/data.qdpx",
        local_filename="data.qdpx",
        file_type="qdpx",
    )
    sink.upsert_asset(pid, asset)
    sink.close()

    conn = sqlite3.connect(tmp_path / "test.sqlite")
    row = conn.execute(
        "SELECT file_name, file_type, status FROM files WHERE project_id = ?",
        (int(pid),),
    ).fetchone()
    conn.close()
    assert row == ("data.qdpx", "qdpx", "FAILED_SERVER_UNRESPONSIVE")


def test_upsert_asset_updates_status(tmp_path: Path) -> None:
    sink = SQLiteSink(name="test", path=tmp_path / "test.sqlite")
    record = _make_record()
    pid = sink.upsert_dataset(record)
    asset = AssetRecord(
        asset_url="https://zenodo.org/files/data.qdpx",
        local_filename="data.qdpx",
        download_status="UNKNOWN",
    )
    sink.upsert_asset(pid, asset)
    asset.download_status = "SUCCEEDED"
    sink.upsert_asset(pid, asset)
    sink.close()

    conn = sqlite3.connect(tmp_path / "test.sqlite")
    row = conn.execute(
        "SELECT status FROM files WHERE project_id = ? AND file_name = ?",
        (int(pid), "data.qdpx"),
    ).fetchone()
    conn.close()
    assert row[0] == "SUCCEEDED"


def test_update_file_status(tmp_path: Path) -> None:
    sink = SQLiteSink(name="test", path=tmp_path / "test.sqlite")
    record = _make_record()
    pid = sink.upsert_dataset(record)
    asset = AssetRecord(
        asset_url="https://zenodo.org/files/data.qdpx",
        local_filename="data.qdpx",
    )
    sink.upsert_asset(pid, asset)
    sink.update_file_status(pid, "data.qdpx", "SUCCEEDED")
    sink.close()

    conn = sqlite3.connect(tmp_path / "test.sqlite")
    row = conn.execute(
        "SELECT status FROM files WHERE project_id = ? AND file_name = ?",
        (int(pid), "data.qdpx"),
    ).fetchone()
    conn.close()
    assert row[0] == "SUCCEEDED"


def test_get_file_statuses(tmp_path: Path) -> None:
    sink = SQLiteSink(name="test", path=tmp_path / "test.sqlite")
    record = _make_record()
    pid = sink.upsert_dataset(record)
    sink.upsert_asset(
        pid, AssetRecord(asset_url="u1", local_filename="a.qdpx", download_status="SUCCEEDED")
    )
    sink.upsert_asset(
        pid,
        AssetRecord(
            asset_url="u2", local_filename="b.pdf", download_status="FAILED_SERVER_UNRESPONSIVE"
        ),
    )
    # Transient (RESUMABLE) is mapped to the failure default on insert.
    sink.upsert_asset(
        pid, AssetRecord(asset_url="u3", local_filename="c.csv", download_status="RESUMABLE")
    )

    statuses = sink.get_file_statuses(pid)
    sink.close()

    # Keys include both file_name and asset_url for flexible lookup
    assert statuses == {
        "a.qdpx": "SUCCEEDED",
        "u1": "SUCCEEDED",
        "b.pdf": "FAILED_SERVER_UNRESPONSIVE",
        "u2": "FAILED_SERVER_UNRESPONSIVE",
        "c.csv": "FAILED_SERVER_UNRESPONSIVE",
        "u3": "FAILED_SERVER_UNRESPONSIVE",
    }


def test_different_versions_separate_rows(tmp_path: Path) -> None:
    sink = SQLiteSink(name="test", path=tmp_path / "test.sqlite")
    r1 = _make_record(version="v1", download_version_folder="v1")
    r2 = _make_record(version="v2", download_version_folder="v2")
    pid1 = sink.upsert_dataset(r1)
    pid2 = sink.upsert_dataset(r2)
    sink.close()

    conn = sqlite3.connect(tmp_path / "test.sqlite")
    count = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    conn.close()
    assert count == 2
    assert pid1 != pid2
