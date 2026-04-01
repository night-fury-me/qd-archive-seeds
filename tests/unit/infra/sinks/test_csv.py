from __future__ import annotations

import csv
from pathlib import Path

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.infra.sinks.csv_sink import CSVSink, _read_csv


def test_csv_sink_creates_headers(tmp_path: Path) -> None:
    CSVSink(
        name="test",
        dataset_path=tmp_path / "ds.csv",
        asset_path=tmp_path / "as.csv",
    )
    assert (tmp_path / "ds.csv").exists()
    with (tmp_path / "ds.csv").open() as f:
        reader = csv.reader(f)
        header = next(reader)
    assert "source_name" in header


def test_csv_sink_upsert_deduplicates(tmp_path: Path) -> None:
    sink = CSVSink(
        name="test",
        dataset_path=tmp_path / "ds.csv",
        asset_path=tmp_path / "as.csv",
    )
    record = DatasetRecord(
        source_name="test",
        source_dataset_id="1",
        source_url="https://example.com",
        title="Test",
    )
    sink.upsert_dataset(record)
    sink.upsert_dataset(record)
    sink.close()
    with (tmp_path / "ds.csv").open() as f:
        lines = list(csv.reader(f))
    assert len(lines) == 2  # header + 1 row (upsert replaces existing)


def test_csv_sink_upsert_updates_fields(tmp_path: Path) -> None:
    sink = CSVSink(
        name="test",
        dataset_path=tmp_path / "ds.csv",
        asset_path=tmp_path / "as.csv",
    )
    record1 = DatasetRecord(
        source_name="test",
        source_dataset_id="1",
        source_url="https://example.com",
        title="Old Title",
    )
    record2 = DatasetRecord(
        source_name="test",
        source_dataset_id="1",
        source_url="https://example.com",
        title="New Title",
    )
    sink.upsert_dataset(record1)
    sink.upsert_dataset(record2)
    sink.close()
    with (tmp_path / "ds.csv").open() as f:
        lines = list(csv.reader(f))
    assert len(lines) == 2  # header + 1 row
    assert lines[1][4] == "New Title"  # title column


def test_csv_sink_upsert_asset_writes_row(tmp_path: Path) -> None:
    sink = CSVSink(
        name="test",
        dataset_path=tmp_path / "ds.csv",
        asset_path=tmp_path / "as.csv",
    )
    asset = AssetRecord(asset_url="https://example.com/file.pdf", asset_type="document")
    sink.upsert_asset("ds-1", asset)
    sink.close()

    with (tmp_path / "as.csv").open() as f:
        lines = list(csv.reader(f))
    assert len(lines) == 2
    assert lines[1][0] == "https://example.com/file.pdf"


def test_read_csv_missing_or_empty(tmp_path: Path) -> None:
    missing = tmp_path / "missing.csv"
    headers, rows = _read_csv(missing, "id")
    assert headers == []
    assert rows == {}

    empty = tmp_path / "empty.csv"
    empty.write_text("")
    headers, rows = _read_csv(empty, "id")
    assert headers == []
    assert rows == {}
