from __future__ import annotations

from pathlib import Path

import pandas as pd  # type: ignore[import-untyped]

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.infra.sinks.excel_sink import ExcelSink


def test_excel_sink_upsert_dataset_and_asset(tmp_path: Path) -> None:
    path = tmp_path / "db.xlsx"
    sink = ExcelSink(name="excel", path=path)

    record = DatasetRecord(
        source_name="s",
        source_dataset_id=1,  # type: ignore[arg-type]
        source_url="https://example.com",
        title="Title",
    )
    dataset_id = sink.upsert_dataset(record)
    asset = AssetRecord(asset_url="https://example.com/file.pdf", asset_type="document")
    sink.upsert_asset(dataset_id, asset)
    sink.upsert_asset(dataset_id, asset)

    assert path.exists()
    datasets = pd.read_excel(path, sheet_name="datasets")
    assets = pd.read_excel(path, sheet_name="assets")
    assert len(datasets) == 1
    assert len(assets) == 1
    assert str(datasets.iloc[0]["id"]) == "1"
    assert assets.iloc[0]["asset_url"] == "https://example.com/file.pdf"


def test_excel_sink_updates_existing_rows(tmp_path: Path) -> None:
    path = tmp_path / "db.xlsx"
    sink = ExcelSink(name="excel", path=path)

    record1 = DatasetRecord(
        source_name="s",
        source_dataset_id=1,  # type: ignore[arg-type]
        source_url="https://example.com",
        title="Old",
    )
    record2 = DatasetRecord(
        source_name="s",
        source_dataset_id=1,  # type: ignore[arg-type]
        source_url="https://example.com",
        title="New",
    )

    sink.upsert_dataset(record1)
    sink.upsert_dataset(record2)

    datasets = pd.read_excel(path, sheet_name="datasets")
    assert len(datasets) == 1
    assert datasets.iloc[0]["title"] == "New"
