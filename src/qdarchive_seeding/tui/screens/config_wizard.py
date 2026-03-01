from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import Button, Footer, Header, Input, Static

from qdarchive_seeding.tui.widgets.forms.auth_form import AuthForm
from qdarchive_seeding.tui.widgets.forms.logging_form import LoggingForm
from qdarchive_seeding.tui.widgets.forms.sink_form import SinkForm
from qdarchive_seeding.tui.widgets.forms.source_form import SourceForm
from qdarchive_seeding.tui.widgets.forms.storage_form import StorageForm
from qdarchive_seeding.tui.widgets.forms.transforms_form import TransformsForm
from qdarchive_seeding.tui.widgets.preview_yaml import PreviewYaml

STEPS = ["Source", "Auth", "Extractor", "Transforms", "Storage", "Sink", "Logging", "Preview"]


class ConfigWizardScreen(Screen[None]):
    BINDINGS = [("escape", "pop_screen", "Back")]

    def __init__(self) -> None:
        super().__init__()
        self._step = 0
        self._config_data: dict[str, Any] = {}

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal(id="wizard"):
            with Vertical(id="wizard-nav"):
                for i, name in enumerate(STEPS):
                    yield Button(name, id=f"step-{i}", classes="step-btn")
            with Vertical(id="wizard-form"):
                yield SourceForm(id="form-source")
                yield AuthForm(id="form-auth", classes="hidden")
                yield Vertical(
                    Static("[bold]Extractor[/bold]"),
                    Static("Name:"),
                    Input(placeholder="e.g. zenodo_extractor", id="extractor-name"),
                    id="form-extractor",
                    classes="hidden",
                )
                yield TransformsForm(id="form-transforms", classes="hidden")
                yield StorageForm(id="form-storage", classes="hidden")
                yield SinkForm(id="form-sink", classes="hidden")
                yield LoggingForm(id="form-logging", classes="hidden")
                yield Vertical(
                    Static("[bold]Preview & Save[/bold]"),
                    PreviewYaml(id="yaml-preview"),
                    Static("Save path:"),
                    Input(value="configs/my_config.yaml", id="save-path"),
                    Button("Save", id="btn-save", variant="primary"),
                    id="form-preview",
                    classes="hidden",
                )
            with Horizontal(id="wizard-buttons"):
                yield Button("Previous", id="btn-prev")
                yield Button("Next", id="btn-next", variant="primary")
        yield Footer()

    def on_mount(self) -> None:
        self._show_step(0)

    def _show_step(self, step: int) -> None:
        form_ids = [
            "form-source",
            "form-auth",
            "form-extractor",
            "form-transforms",
            "form-storage",
            "form-sink",
            "form-logging",
            "form-preview",
        ]
        for i, fid in enumerate(form_ids):
            widget = self.query_one(f"#{fid}")
            widget.set_class(i != step, "hidden")
        self._step = step
        if step == len(STEPS) - 1:
            self._build_config()
            preview = self.query_one("#yaml-preview", PreviewYaml)
            preview.update_config(self._config_data)

    def _build_config(self) -> None:
        source = self.query_one("#form-source", SourceForm).get_data()
        auth = self.query_one("#form-auth", AuthForm).get_data()
        extractor_name = self.query_one("#extractor-name", Input).value or "generic_rest_extractor"
        transforms = self.query_one("#form-transforms", TransformsForm).get_data()
        storage = self.query_one("#form-storage", StorageForm).get_data()
        sink = self.query_one("#form-sink", SinkForm).get_data()
        logging_cfg = self.query_one("#form-logging", LoggingForm).get_data()

        self._config_data = {
            "pipeline": {"id": f"{source.get('name', 'pipeline')}_v1", "run_mode": "incremental"},
            "source": source,
            "auth": auth,
            "extractor": {"name": extractor_name, "options": {}},
            "transforms": transforms,
            "storage": storage,
            "sink": sink,
            "logging": logging_cfg,
        }

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-next" and self._step < len(STEPS) - 1:
            self._show_step(self._step + 1)
        elif event.button.id == "btn-prev" and self._step > 0:
            self._show_step(self._step - 1)
        elif event.button.id and event.button.id.startswith("step-"):
            step_num = int(event.button.id.split("-")[1])
            self._show_step(step_num)
        elif event.button.id == "btn-save":
            self._save_config()

    def _save_config(self) -> None:
        path_input = self.query_one("#save-path", Input)
        path = Path(path_input.value)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.dump(self._config_data, default_flow_style=False, sort_keys=False))
        self.notify(f"Config saved to {path}")

    def action_pop_screen(self) -> None:
        self.app.pop_screen()
