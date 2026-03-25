from __future__ import annotations

import pytest

import csv
from pathlib import Path
from typing import Any

import yaml

from qdarchive_seeding.app.config_loader import load_config
from qdarchive_seeding.app.container import build_container
from qdarchive_seeding.app.runner import ETLRunner


@pytest.mark.asyncio



async def test_static_list_to_csv(tmp_path: Path) -> None:
    config_dict: dict[str, Any] = {
        "pipeline": {"id": "csv_test", "run_mode": "full"},
        "source": {
            "name": "test_source",
            "type": "static_list",
            "base_url": "https://example.com",
        },
        "auth": {"type": "none"},
        "extractor": {
            "name": "static_list_extractor",
            "options": {
                "records": [
                    {
                        "id": "ds-1",
                        "source_url": "https://example.com/1",
                        "title": "CSV Test",
                        "assets": [],
                    }
                ]
            },
        },
        "transforms": [{"name": "slugify_dataset"}],
        "storage": {
            "downloads_root": str(tmp_path / "downloads"),
            "layout": "{source_name}/{dataset_slug}/",
        },
        "sink": {
            "type": "csv",
            "options": {
                "dataset_path": str(tmp_path / "datasets.csv"),
                "asset_path": str(tmp_path / "assets.csv"),
            },
        },
        "logging": {"level": "WARNING"},
    }

    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(config_dict))
    config = load_config(config_path)

    container = build_container(config, runs_dir=tmp_path / "runs")
    runner = ETLRunner(container)
    info = await runner.run(dry_run=False)

    assert info.counts["extracted"] == 1
    with (tmp_path / "datasets.csv").open() as f:
        rows = list(csv.reader(f))
    assert len(rows) == 2  # header + 1 data row
    assert rows[1][3] == "https://example.com/1"  # source_url
