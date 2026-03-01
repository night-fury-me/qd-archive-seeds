from __future__ import annotations

from typing import Any

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import Input, Select, Static


class StorageForm(Vertical):
    def compose(self) -> ComposeResult:
        yield Static("[bold]Storage Configuration[/bold]")
        yield Static("Downloads root:")
        yield Input(value="./downloads", id="storage-root")
        yield Static("Layout template:")
        yield Input(value="{source_name}/{dataset_slug}/", id="storage-layout")
        yield Static("Checksum:")
        yield Select(
            [("SHA-256", "sha256"), ("None", "none")],
            value="sha256",
            id="storage-checksum",
        )

    def get_data(self) -> dict[str, Any]:
        return {
            "downloads_root": self.query_one("#storage-root", Input).value,
            "layout": self.query_one("#storage-layout", Input).value,
            "checksum": str(self.query_one("#storage-checksum", Select).value or "sha256"),
        }
