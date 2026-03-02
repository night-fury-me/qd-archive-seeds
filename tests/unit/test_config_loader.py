from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from qdarchive_seeding.app.config_loader import config_hash, load_config
from qdarchive_seeding.core.exceptions import ConfigError


def test_load_valid_config(config_yaml_path: Path) -> None:
    cfg = load_config(config_yaml_path)
    assert cfg.pipeline.id == "test_pipeline"
    assert cfg.source.name == "test"


def test_load_missing_file() -> None:
    with pytest.raises(ConfigError, match="Config not found"):
        load_config("/nonexistent/path.yaml")


def test_load_invalid_yaml(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text("- :\n  :\n- {a: [}")
    with pytest.raises(ConfigError):
        load_config(bad)


def test_load_root_not_mapping(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text("- item1\n- item2\n")
    with pytest.raises(ConfigError, match="Config root must be a mapping"):
        load_config(bad)


def test_load_extra_field_rejected(tmp_path: Path, minimal_config_dict: dict[str, Any]) -> None:
    minimal_config_dict["pipeline"]["unknown_field"] = "bad"
    path = tmp_path / "extra.yaml"
    path.write_text(yaml.dump(minimal_config_dict))
    with pytest.raises(ConfigError):
        load_config(path)


def test_load_missing_required_field(tmp_path: Path) -> None:
    incomplete = {"pipeline": {"id": "test"}}
    path = tmp_path / "incomplete.yaml"
    path.write_text(yaml.dump(incomplete))
    with pytest.raises(ConfigError):
        load_config(path)


def test_config_hash_stable(config_yaml_path: Path) -> None:
    cfg = load_config(config_yaml_path)
    h1 = config_hash(cfg)
    h2 = config_hash(cfg)
    assert h1 == h2
    assert len(h1) == 64
