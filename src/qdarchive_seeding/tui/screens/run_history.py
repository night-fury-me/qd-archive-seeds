from __future__ import annotations

from pathlib import Path

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import Screen
from textual.widgets import Button, DataTable, Footer, Header, Static

from qdarchive_seeding.app.manifests import RunManifestWriter


class RunHistoryScreen(Screen[None]):
    BINDINGS = [("escape", "pop_screen", "Back")]

    def __init__(self, runs_dir: Path = Path("runs")) -> None:
        super().__init__()
        self._manifests = RunManifestWriter(runs_dir=runs_dir)

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical(id="history"):
            yield Static("[bold]Run History[/bold]")
            yield DataTable(id="history-table")
            yield Button("Refresh", id="btn-refresh")
            yield Static("", id="history-detail")
        yield Footer()

    def on_mount(self) -> None:
        self._load_runs()

    def _load_runs(self) -> None:
        table = self.query_one("#history-table", DataTable)
        table.clear(columns=True)
        table.add_columns("Run ID", "Pipeline", "Started", "Extracted", "Downloaded", "Failed")
        runs = self._manifests.list_runs()
        for run in runs:
            counts = run.get("counts", {})
            table.add_row(
                str(run.get("run_id", ""))[:12],
                str(run.get("pipeline_id", "")),
                str(run.get("started_at", ""))[:19],
                str(counts.get("extracted", 0)),
                str(counts.get("downloaded", 0)),
                str(counts.get("failed", 0)),
            )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-refresh":
            self._load_runs()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        table = self.query_one("#history-table", DataTable)
        cells = list(table.get_row(event.row_key))
        if cells:
            run_id_short = cells[0]
            detail = self.query_one("#history-detail", Static)
            runs = self._manifests.list_runs()
            for run in runs:
                if str(run.get("run_id", "")).startswith(str(run_id_short)):
                    failures = run.get("failures", [])
                    text = (
                        f"[bold]Run {run.get('run_id')}[/bold]\n"
                        f"Pipeline: {run.get('pipeline_id')}\n"
                        f"Started: {run.get('started_at')}\n"
                        f"Ended: {run.get('ended_at')}\n"
                        f"Counts: {run.get('counts')}\n"
                        f"Failures: {len(failures)}"
                    )
                    detail.update(text)
                    break

    def action_pop_screen(self) -> None:
        self.app.pop_screen()
