from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import yaml

from qdarchive_seeding.app.config_models import PipelineConfig
from qdarchive_seeding.core.exceptions import ConfigError


def load_config(path: str | Path) -> PipelineConfig:
    file_path = Path(path)
    if not file_path.exists():
        raise ConfigError(f"Config not found: {file_path}")

    try:
        raw = yaml.safe_load(file_path.read_text())
    except yaml.YAMLError as exc:
        raise ConfigError("Invalid YAML configuration") from exc

    if not isinstance(raw, dict):
        raise ConfigError("Config root must be a mapping")

    try:
        return PipelineConfig.model_validate(raw)
    except Exception as exc:  # pydantic error
        raise ConfigError(str(exc)) from exc


def config_hash(config: PipelineConfig) -> str:
    payload = config.model_dump_json().encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
