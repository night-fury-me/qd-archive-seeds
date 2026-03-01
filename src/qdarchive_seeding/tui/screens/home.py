from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Center, Vertical
from textual.screen import Screen
from textual.widgets import Button, Footer, Header, Static


class HomeScreen(Screen[None]):
    BINDINGS = [("q", "quit", "Quit")]

    def compose(self) -> ComposeResult:
        yield Header()
        with Center(), Vertical(id="home-menu"):
            yield Static("[bold]QDArchive Seeding[/bold]", id="title")
            yield Button("Run Pipeline", id="btn-run", variant="primary")
            yield Button("Config Wizard", id="btn-wizard")
            yield Button("Config Editor", id="btn-editor")
            yield Button("Validate Config", id="btn-validate")
            yield Button("Run History", id="btn-history")
            yield Button("Browse Downloads", id="btn-browse")
            yield Button("Settings", id="btn-settings")
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_map = {
            "btn-run": "run_select",
            "btn-wizard": "config_wizard",
            "btn-editor": "config_editor",
            "btn-validate": "config_validate",
            "btn-history": "run_history",
            "btn-browse": "browse_downloads",
            "btn-settings": "settings",
        }
        screen_name = button_map.get(event.button.id or "")
        if screen_name:
            self.app.push_screen(screen_name)

    def action_quit(self) -> None:
        self.app.exit()
