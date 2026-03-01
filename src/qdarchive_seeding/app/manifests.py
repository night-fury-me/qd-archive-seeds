from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from qdarchive_seeding.core.entities import RunInfo


@dataclass(slots=True)
class RunManifestWriter:
    runs_dir: Path

    def __post_init__(self) -> None:
        self.runs_dir.mkdir(parents=True, exist_ok=True)

    def write(self, run_info: RunInfo) -> Path:
        manifest = _run_info_to_dict(run_info)
        path = self.runs_dir / f"{run_info.run_id}.json"
        path.write_text(json.dumps(manifest, indent=2, default=str))
        return path

    def load(self, run_id: str) -> dict[str, Any]:
        path = self.runs_dir / f"{run_id}.json"
        return json.loads(path.read_text())  # type: ignore[no-any-return]

    def list_runs(self) -> list[dict[str, Any]]:
        runs: list[dict[str, Any]] = []
        for path in sorted(self.runs_dir.glob("*.json"), reverse=True):
            try:
                data = json.loads(path.read_text())
                runs.append(data)
            except (json.JSONDecodeError, OSError):
                continue
        runs.sort(key=lambda r: r.get("started_at", ""), reverse=True)
        return runs


def _run_info_to_dict(run_info: RunInfo) -> dict[str, Any]:
    return {
        "run_id": str(run_info.run_id),
        "pipeline_id": run_info.pipeline_id,
        "started_at": run_info.started_at.isoformat() if run_info.started_at else None,
        "ended_at": run_info.ended_at.isoformat() if run_info.ended_at else None,
        "config_hash": run_info.config_hash,
        "counts": run_info.counts,
        "failures": run_info.failures,
        "environment": run_info.environment,
    }
