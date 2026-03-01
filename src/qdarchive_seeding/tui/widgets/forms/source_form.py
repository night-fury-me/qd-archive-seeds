from __future__ import annotations

from typing import Any

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import Input, Select, Static


class SourceForm(Vertical):
    def compose(self) -> ComposeResult:
        yield Static("[bold]Source Configuration[/bold]")
        yield Static("Name:")
        yield Input(placeholder="e.g. zenodo", id="source-name")
        yield Static("Type:")
        yield Select(
            [("REST API", "rest_api"), ("HTML Scraper", "html"), ("Static List", "static_list")],
            prompt="Source type",
            id="source-type",
        )
        yield Static("Base URL:")
        yield Input(placeholder="https://api.example.com", id="source-base-url")
        yield Static("Search Endpoint:")
        yield Input(placeholder="/records", id="source-endpoint")

    def get_data(self) -> dict[str, Any]:
        return {
            "name": self.query_one("#source-name", Input).value,
            "type": str(self.query_one("#source-type", Select).value or "rest_api"),
            "base_url": self.query_one("#source-base-url", Input).value,
            "endpoints": {"search": self.query_one("#source-endpoint", Input).value},
            "params": {},
        }
