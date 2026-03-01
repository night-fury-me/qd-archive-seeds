from __future__ import annotations

from pathlib import Path

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import Screen
from textual.widgets import Button, Footer, Header, OptionList, Select, Static

from qdarchive_seeding.tui.services.config_service import ConfigService


class RunSelectScreen(Screen[None]):
    BINDINGS = [("escape", "pop_screen", "Back")]

    def __init__(self) -> None:
        super().__init__()
        self._config_service = ConfigService()
        self._selected_config: Path | None = None

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical(id="run-select"):
            yield Static("[bold]Select Config & Run[/bold]")
            configs = self._config_service.list_configs()
            options = [(str(c), str(c)) for c in configs]
            yield Select(options, prompt="Select a config file", id="config-select")
            yield Select(
                [
                    ("Dry Run", "dry_run"),
                    ("Incremental", "incremental"),
                    ("Full", "full"),
                    ("Force Re-download", "force"),
                ],
                prompt="Run mode",
                value="incremental",
                id="mode-select",
            )
            yield Button("Start Run", id="btn-start", variant="primary")
        yield Footer()

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id == "config-select" and event.value is not None:
            self._selected_config = Path(str(event.value))

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-start" and self._selected_config:
            mode_select = self.query_one("#mode-select", Select)
            mode = str(mode_select.value) if mode_select.value else "incremental"
            self.app.push_screen(
                "run_monitor",
                {  # type: ignore[arg-type]
                    "config_path": self._selected_config,
                    "dry_run": mode == "dry_run",
                    "force": mode == "force",
                },
            )

    def action_pop_screen(self) -> None:
        self.app.pop_screen()
