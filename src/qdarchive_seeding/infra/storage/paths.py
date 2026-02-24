from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class PathStrategy:
    layout_template: str

    def dataset_dir(self, root: Path, *, source_name: str, dataset_slug: str) -> Path:
        layout = self.layout_template.format(source_name=source_name, dataset_slug=dataset_slug)
        return root / layout

    def asset_path(
        self,
        root: Path,
        *,
        source_name: str,
        dataset_slug: str,
        filename: str,
    ) -> Path:
        return self.dataset_dir(root, source_name=source_name, dataset_slug=dataset_slug) / filename


def safe_filename(name: str, fallback: str = "file") -> str:
    cleaned = "".join(ch for ch in name if ch.isalnum() or ch in {"-", "_", "."})
    return cleaned or fallback
