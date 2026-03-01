from __future__ import annotations

import os

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import Screen
from textual.widgets import Footer, Header, Input, Static, Switch


ENV_VARS_TO_CHECK = ["ZENODO_TOKEN", "MYSQL_PASSWORD", "MONGODB_URI"]


class SettingsScreen(Screen[None]):
    BINDINGS = [("escape", "pop_screen", "Back")]

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical(id="settings"):
            yield Static("[bold]Settings[/bold]")
            yield Static("Downloads root:")
            yield Input(value="./downloads", id="settings-downloads-root")
            yield Static("Metadata DB:")
            yield Input(value="./metadata/qdarchive.sqlite", id="settings-db-path")
            yield Static("Runs directory:")
            yield Input(value="./runs", id="settings-runs-dir")
            yield Static("")
            yield Static("[bold]Environment Variables[/bold]")
            for var in ENV_VARS_TO_CHECK:
                present = os.environ.get(var) is not None
                color = "green" if present else "red"
                status = "SET" if present else "NOT SET"
                yield Static(f"[{color}]{var}: {status}[/{color}]")
        yield Footer()

    def action_pop_screen(self) -> None:
        self.app.pop_screen()
