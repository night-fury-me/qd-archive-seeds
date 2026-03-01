from __future__ import annotations

from pathlib import Path

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import Button, Footer, Header, Input, Select, Static, TextArea

from qdarchive_seeding.app.config_loader import load_config
from qdarchive_seeding.core.exceptions import ConfigError
from qdarchive_seeding.tui.services.config_service import ConfigService


class ConfigEditorScreen(Screen[None]):
    BINDINGS = [("escape", "pop_screen", "Back")]

    def __init__(self) -> None:
        super().__init__()
        self._config_service = ConfigService()
        self._current_path: Path | None = None

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical(id="editor"):
            configs = self._config_service.list_configs()
            options = [(str(c), str(c)) for c in configs]
            yield Select(options, prompt="Open config file", id="editor-file-select")
            yield TextArea("", language="yaml", id="editor-text")
            with Horizontal(id="editor-actions"):
                yield Button("Validate", id="btn-validate")
                yield Button("Save", id="btn-save", variant="primary")
                yield Static("Save as:")
                yield Input(placeholder="path/to/save.yaml", id="editor-save-path")
            yield Static("", id="editor-status")
        yield Footer()

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id == "editor-file-select" and event.value:
            path = Path(str(event.value))
            self._current_path = path
            try:
                content = path.read_text()
                self.query_one("#editor-text", TextArea).load_text(content)
                self.query_one("#editor-save-path", Input).value = str(path)
                self.query_one("#editor-status", Static).update("")
            except Exception as exc:
                self.query_one("#editor-status", Static).update(f"[red]Error: {exc}[/red]")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-validate":
            self._validate()
        elif event.button.id == "btn-save":
            self._save()

    def _validate(self) -> None:
        text = self.query_one("#editor-text", TextArea).text
        import tempfile

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(text)
            f.flush()
            try:
                load_config(f.name)
                self.query_one("#editor-status", Static).update("[green]Valid[/green]")
            except ConfigError as exc:
                self.query_one("#editor-status", Static).update(f"[red]Invalid: {exc}[/red]")

    def _save(self) -> None:
        save_path = self.query_one("#editor-save-path", Input).value
        if not save_path:
            self.query_one("#editor-status", Static).update("[yellow]No save path[/yellow]")
            return
        path = Path(save_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        text = self.query_one("#editor-text", TextArea).text
        path.write_text(text)
        self.query_one("#editor-status", Static).update(f"[green]Saved to {path}[/green]")

    def action_pop_screen(self) -> None:
        self.app.pop_screen()
