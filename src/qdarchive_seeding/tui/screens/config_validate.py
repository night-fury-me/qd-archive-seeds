from __future__ import annotations

from pathlib import Path

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import Screen
from textual.widgets import Button, Footer, Header, Select, Static

from qdarchive_seeding.app.config_loader import load_config
from qdarchive_seeding.core.exceptions import ConfigError
from qdarchive_seeding.tui.services.config_service import ConfigService


class ConfigValidateScreen(Screen[None]):
    BINDINGS = [("escape", "pop_screen", "Back")]

    def __init__(self) -> None:
        super().__init__()
        self._config_service = ConfigService()

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical(id="validate"):
            yield Static("[bold]Validate Configuration[/bold]")
            configs = self._config_service.list_configs()
            options = [(str(c), str(c)) for c in configs]
            yield Select(options, prompt="Select config to validate", id="validate-select")
            yield Button("Validate", id="btn-validate", variant="primary")
            yield Static("", id="validate-result")
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-validate":
            self._do_validate()

    def _do_validate(self) -> None:
        select = self.query_one("#validate-select", Select)
        if not select.value:
            self.query_one("#validate-result", Static).update("[yellow]No config selected[/yellow]")
            return
        path = Path(str(select.value))
        try:
            load_config(path)
            self.query_one("#validate-result", Static).update("[green]Valid[/green]")
        except ConfigError as exc:
            self.query_one("#validate-result", Static).update(f"[red]Invalid:\n{exc}[/red]")

    def action_pop_screen(self) -> None:
        self.app.pop_screen()
