from __future__ import annotations

from typing import Any

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import Checkbox, Static

AVAILABLE_TRANSFORMS = [
    ("validate_required_fields", "Validate Required Fields"),
    ("normalize_fields", "Normalize Fields"),
    ("infer_filetypes", "Infer File Types"),
    ("deduplicate_assets", "Deduplicate Assets"),
    ("slugify_dataset", "Slugify Dataset"),
]


class TransformsForm(Vertical):
    def compose(self) -> ComposeResult:
        yield Static("[bold]Transforms[/bold]")
        yield Static("Enable transforms:")
        for name, label in AVAILABLE_TRANSFORMS:
            yield Checkbox(label, value=True, id=f"transform-{name}")

    def get_data(self) -> list[dict[str, Any]]:
        transforms: list[dict[str, Any]] = []
        for name, _ in AVAILABLE_TRANSFORMS:
            cb = self.query_one(f"#transform-{name}", Checkbox)
            if cb.value:
                transforms.append({"name": name})
        return transforms
