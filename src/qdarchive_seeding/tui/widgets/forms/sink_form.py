from __future__ import annotations

from typing import Any

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import Input, Select, Static


class SinkForm(Vertical):
    def compose(self) -> ComposeResult:
        yield Static("[bold]Sink Configuration[/bold]")
        yield Static("Sink type:")
        yield Select(
            [
                ("SQLite", "sqlite"),
                ("MySQL", "mysql"),
                ("MongoDB", "mongodb"),
                ("CSV", "csv"),
                ("Excel", "excel"),
            ],
            prompt="Sink type",
            value="sqlite",
            id="sink-type",
        )
        yield Static("Path / Connection:")
        yield Input(
            placeholder="./metadata/qdarchive.sqlite",
            value="./metadata/qdarchive.sqlite",
            id="sink-path",
        )

    def get_data(self) -> dict[str, Any]:
        sink_type = str(self.query_one("#sink-type", Select).value or "sqlite")
        path = self.query_one("#sink-path", Input).value
        return {"type": sink_type, "options": {"path": path}}
