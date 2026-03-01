from __future__ import annotations

import csv
from pathlib import Path

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.infra.sinks.csv_sink import CSVSink


def test_csv_sink_creates_headers(tmp_path: Path) -> None:
    sink = CSVSink(
        name="test",
        dataset_path=tmp_path / "ds.csv",
        asset_path=tmp_path / "as.csv",
    )
    assert (tmp_path / "ds.csv").exists()
    with (tmp_path / "ds.csv").open() as f:
        reader = csv.reader(f)
        header = next(reader)
    assert "source_name" in header


def test_csv_sink_appends(tmp_path: Path) -> None:
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
    with (tmp_path / "ds.csv").open() as f:
        lines = list(csv.reader(f))
    assert len(lines) == 3  # header + 2 rows (CSV is append-only)
