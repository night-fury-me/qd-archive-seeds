from __future__ import annotations

from typing import Any

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import Checkbox, Input, Select, Static


class LoggingForm(Vertical):
    def compose(self) -> ComposeResult:
        yield Static("[bold]Logging Configuration[/bold]")
        yield Static("Level:")
        yield Select(
            [("DEBUG", "DEBUG"), ("INFO", "INFO"), ("WARNING", "WARNING"), ("ERROR", "ERROR")],
            value="INFO",
            id="log-level",
        )
        yield Checkbox("Console logging", value=True, id="log-console")
        yield Checkbox("File logging", value=True, id="log-file")
        yield Static("Log file path:")
        yield Input(value="./logs/qdarchive.log", id="log-file-path")

    def get_data(self) -> dict[str, Any]:
        return {
            "level": str(self.query_one("#log-level", Select).value or "INFO"),
            "console": {
                "enabled": self.query_one("#log-console", Checkbox).value,
                "rich": True,
            },
            "file": {
                "enabled": self.query_one("#log-file", Checkbox).value,
                "path": self.query_one("#log-file-path", Input).value,
            },
        }
