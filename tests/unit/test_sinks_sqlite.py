from __future__ import annotations

import sqlite3
from pathlib import Path

from qdarchive_seeding.core.constants import DOWNLOAD_STATUS_FAILED, DOWNLOAD_STATUS_SUCCESS
from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.infra.sinks.sqlite import SQLiteSink


def _make_record() -> DatasetRecord:
    return DatasetRecord(
        source_name="test",
        source_dataset_id="ds-1",
        source_url="https://example.com/ds-1",
        title="Test Dataset",
    )


def test_upsert_dataset_idempotent(tmp_path: Path) -> None:
    sink = SQLiteSink(name="test", path=tmp_path / "test.sqlite")
    record = _make_record()
    sink.upsert_dataset(record)
    sink.upsert_dataset(record)
    sink.close()
    conn = sqlite3.connect(tmp_path / "test.sqlite")
    count = conn.execute("SELECT COUNT(*) FROM datasets").fetchone()[0]
    conn.close()
    assert count == 1


def test_upsert_asset_updates(tmp_path: Path) -> None:
    sink = SQLiteSink(name="test", path=tmp_path / "test.sqlite")
    record = _make_record()
    dataset_id = sink.upsert_dataset(record)
    asset = AssetRecord(
        asset_url="https://example.com/file.pdf",
        download_status=DOWNLOAD_STATUS_FAILED,
    )
    sink.upsert_asset(dataset_id, asset)
    asset.download_status = DOWNLOAD_STATUS_SUCCESS
    sink.upsert_asset(dataset_id, asset)
    sink.close()
    conn = sqlite3.connect(tmp_path / "test.sqlite")
    row = conn.execute(
        "SELECT download_status FROM assets WHERE asset_url = ?", (asset.asset_url,)
    ).fetchone()
    conn.close()
    assert row[0] == DOWNLOAD_STATUS_SUCCESS


def test_upsert_dataset_returns_id(tmp_path: Path) -> None:
    sink = SQLiteSink(name="test", path=tmp_path / "test.sqlite")
    record = _make_record()
    dataset_id = sink.upsert_dataset(record)
    sink.close()
    assert dataset_id == "ds-1"
