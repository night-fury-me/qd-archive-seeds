from __future__ import annotations

from pathlib import Path

import yaml

from qdarchive_seeding.app.config_loader import load_config
from qdarchive_seeding.app.config_models import PipelineConfig


class ConfigService:
    def __init__(self, configs_dir: Path = Path("configs")) -> None:
        self._configs_dir = configs_dir

    def list_configs(self) -> list[Path]:
        paths: list[Path] = []
        for pattern in ("*.yaml", "*.yml"):
            paths.extend(self._configs_dir.rglob(pattern))
        return sorted(paths)

    def load(self, path: Path) -> PipelineConfig:
        return load_config(path)

    def save(self, config: PipelineConfig, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        data = config.model_dump(mode="python")
        path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))
