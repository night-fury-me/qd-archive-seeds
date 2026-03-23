from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class PathStrategy:
    layout_template: str

    def dataset_dir(
        self,
        root: Path,
        *,
        source_name: str,
        dataset_slug: str,
        version: str | None = None,
    ) -> Path:
        # Support {version} placeholder in template; fall back to empty string if absent
        layout = self.layout_template.format(
            source_name=source_name,
            dataset_slug=dataset_slug,
            version=version or "",
        )
        # Remove trailing double slashes from empty version
        layout = layout.replace("//", "/")
        return root / layout

    def asset_path(
        self,
        root: Path,
        *,
        source_name: str,
        dataset_slug: str,
        filename: str,
        version: str | None = None,
    ) -> Path:
        return (
            self.dataset_dir(
                root, source_name=source_name, dataset_slug=dataset_slug, version=version
            )
            / filename
        )


def safe_filename(name: str, fallback: str = "file") -> str:
    cleaned = "".join(ch for ch in name if ch.isalnum() or ch in {"-", "_", "."})
    return cleaned or fallback
