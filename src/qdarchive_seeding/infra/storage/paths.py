from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path, PurePosixPath


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
        # Normalize path segments to remove empty parts (e.g. from empty version)
        # Using PurePosixPath avoids the "//" replacement that would break URLs.
        layout = PurePosixPath(*[p for p in layout.split("/") if p]).as_posix()
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
    """Sanitize *name* for use as a filename, preserving Unicode letters.

    Accented characters are decomposed via NFKD so that base letters survive
    even when combining marks are stripped.  ``\\w`` (with Unicode) keeps
    alphanumerics and underscores; we also allow ``-`` and ``.``.
    """
    # Decompose accented characters (e.g. e + combining accent) so base letters survive
    normalized = unicodedata.normalize("NFKD", name)
    # Replace any character that is not a word char, dot, or hyphen with underscore
    cleaned = re.sub(r"[^\w.\-]", "_", normalized)
    # Collapse runs of underscores
    cleaned = re.sub(r"__+", "_", cleaned)
    # Strip leading/trailing underscores
    cleaned = cleaned.strip("_")
    return cleaned or fallback
