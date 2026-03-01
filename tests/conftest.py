from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from qdarchive_seeding.app.config_models import PipelineConfig
from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord


FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture()
def zenodo_response() -> dict[str, Any]:
    return json.loads((FIXTURES_DIR / "zenodo_response.json").read_text())


@pytest.fixture()
def sample_dataset() -> DatasetRecord:
    return DatasetRecord(
        source_name="test_source",
        source_dataset_id="ds-001",
        source_url="https://example.com/datasets/ds-001",
        title="Test Dataset",
        description="A test dataset for unit testing",
        doi="10.1234/test",
        license="MIT",
        year=2024,
        owner_name="Test Owner",
        owner_email="test@example.com",
        assets=[
            AssetRecord(asset_url="https://example.com/files/data.qdpx"),
            AssetRecord(asset_url="https://example.com/files/readme.pdf"),
        ],
        raw={"dataset_slug": "test-dataset"},
    )


@pytest.fixture()
def sample_asset() -> AssetRecord:
    return AssetRecord(
        asset_url="https://example.com/files/data.qdpx",
        asset_type="qdpx",
    )


@pytest.fixture()
def minimal_config_dict() -> dict[str, Any]:
    return {
        "pipeline": {"id": "test_pipeline", "run_mode": "incremental"},
        "source": {
            "name": "test",
            "type": "rest_api",
            "base_url": "https://api.example.com",
        },
        "auth": {"type": "none"},
        "extractor": {"name": "static_list_extractor", "options": {"records": []}},
        "transforms": [],
        "storage": {
            "downloads_root": "./downloads",
            "layout": "{source_name}/{dataset_slug}/",
        },
        "sink": {"type": "sqlite", "options": {"path": "./test.sqlite"}},
        "logging": {"level": "WARNING"},
    }


@pytest.fixture()
def minimal_config(minimal_config_dict: dict[str, Any]) -> PipelineConfig:
    return PipelineConfig.model_validate(minimal_config_dict)


@pytest.fixture()
def config_yaml_path(tmp_path: Path, minimal_config_dict: dict[str, Any]) -> Path:
    path = tmp_path / "test_config.yaml"
    path.write_text(yaml.dump(minimal_config_dict))
    return path
