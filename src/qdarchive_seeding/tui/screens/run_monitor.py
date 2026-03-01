from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.message import Message
from textual.screen import Screen
from textual.widgets import Button, Footer, Header, Static

from qdarchive_seeding.app.progress import (
    AssetDownloadUpdate,
    Completed,
    CountersUpdated,
    ErrorEvent,
    ProgressEvent,
    StageChanged,
)
from qdarchive_seeding.tui.services.run_service import RunService
from qdarchive_seeding.tui.widgets.log_viewer import LogViewerWidget
from qdarchive_seeding.tui.widgets.progress_panel import ProgressPanel
from qdarchive_seeding.tui.widgets.run_table import RunTable


@dataclass
class PipelineEvent(Message):
    event: ProgressEvent


class RunMonitorScreen(Screen[None]):
    BINDINGS = [("escape", "pop_screen", "Back")]

    def __init__(
        self,
        config_path: Path | None = None,
        dry_run: bool = False,
        force: bool = False,
    ) -> None:
        super().__init__()
        self._config_path = config_path or Path("configs/examples/zenodo.yaml")
        self._dry_run = dry_run
        self._force = force
        self._run_service = RunService()

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical(id="monitor"):
            yield ProgressPanel(id="progress")
            with Horizontal(id="monitor-body"):
                yield RunTable(id="downloads")
                yield LogViewerWidget(id="log-viewer")
            with Horizontal(id="monitor-actions"):
                yield Button("Stop", id="btn-stop", variant="error")
                yield Static("", id="status-label")
        yield Footer()

    async def on_mount(self) -> None:
        self.run_worker(self._start_pipeline(), exclusive=True)

    async def _start_pipeline(self) -> None:
        def on_event(event: ProgressEvent) -> None:
            self.post_message(PipelineEvent(event=event))

        try:
            await self._run_service.run(
                self._config_path,
                dry_run=self._dry_run,
                force=self._force,
                on_event=on_event,
            )
        except Exception as exc:
            self.query_one("#status-label", Static).update(f"[red]Error: {exc}[/red]")

    def on_pipeline_event(self, message: PipelineEvent) -> None:
        event = message.event
        progress = self.query_one("#progress", ProgressPanel)
        if isinstance(event, StageChanged):
            progress.stage = event.stage
        elif isinstance(event, CountersUpdated):
            progress.extracted = event.extracted
            progress.transformed = event.transformed
            progress.downloaded = event.downloaded
            progress.failed = event.failed
            progress.skipped = event.skipped
        elif isinstance(event, AssetDownloadUpdate):
            table = self.query_one("#downloads", RunTable)
            table.update_asset(event.asset_url, event.status, event.bytes_downloaded)
        elif isinstance(event, ErrorEvent):
            table = self.query_one("#downloads", RunTable)
            if event.asset_url:
                table.update_asset(event.asset_url, "ERROR", error=event.message)
        elif isinstance(event, Completed):
            progress.stage = "done"
            self.query_one("#status-label", Static).update("[green]Run completed[/green]")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-stop":
            self._run_service.cancel()
            self.query_one("#status-label", Static).update("[yellow]Stopping...[/yellow]")

    def action_pop_screen(self) -> None:
        self.app.pop_screen()
