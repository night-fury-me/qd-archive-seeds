from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import yaml

from qdarchive_seeding.app.config_loader import load_config
from qdarchive_seeding.app.container import build_container
from qdarchive_seeding.app.runner import ETLRunner


def test_static_list_to_sqlite(tmp_path: Path) -> None:
    config_dict: dict[str, Any] = {
        "pipeline": {"id": "integration_test", "run_mode": "full"},
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
                        "id": "ds-001",
                        "source_url": "https://example.com/ds/001",
                        "title": "Integration Test Dataset",
                        "description": "A test dataset",
                        "assets": [],
                    },
                    {
                        "id": "ds-002",
                        "source_url": "https://example.com/ds/002",
                        "title": "Second Dataset",
                        "assets": [],
                    },
                ]
            },
        },
        "transforms": [
            {"name": "validate_required_fields", "options": {"required_fields": ["source_url"]}},
            {"name": "normalize_fields"},
            {"name": "slugify_dataset"},
        ],
        "storage": {
            "downloads_root": str(tmp_path / "downloads"),
            "layout": "{source_name}/{dataset_slug}/",
        },
        "sink": {
            "type": "sqlite",
            "options": {"path": str(tmp_path / "test.sqlite")},
        },
        "logging": {"level": "WARNING"},
    }

    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(config_dict))
    config = load_config(config_path)

    container = build_container(config, runs_dir=tmp_path / "runs")
    runner = ETLRunner(container)
    info = runner.run(dry_run=False)

    assert info.counts["extracted"] == 2
    assert info.counts["transformed"] == 2

    conn = sqlite3.connect(tmp_path / "test.sqlite")
    projects = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    conn.close()
    assert projects == 2
