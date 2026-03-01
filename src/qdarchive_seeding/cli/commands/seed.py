from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.table import Table

from qdarchive_seeding.app.config_loader import load_config
from qdarchive_seeding.app.container import build_container
from qdarchive_seeding.app.progress import (
    Completed,
    CountersUpdated,
    ErrorEvent,
    ProgressEvent,
    StageChanged,
)
from qdarchive_seeding.app.runner import ETLRunner
from qdarchive_seeding.core.exceptions import ConfigError

seed_app = typer.Typer(help="Seed pipeline commands.")
console = Console()


@seed_app.command("run")
def run_pipeline(
    config: Annotated[Path, typer.Option("--config", help="Path to YAML config file")],
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Run without downloading")] = False,
    force: Annotated[bool, typer.Option("--force", help="Force re-download all")] = False,
    max_items: Annotated[Optional[int], typer.Option("--max-items", help="Override max items")] = None,
    retry_failed: Annotated[bool, typer.Option("--retry-failed", help="Retry failed downloads")] = False,
) -> None:
    """Run a seeding pipeline from a YAML config."""
    try:
        cfg = load_config(config)
    except ConfigError as exc:
        console.print(f"[red]Config error:[/red] {exc}")
        raise typer.Exit(code=1) from exc

    if max_items is not None:
        cfg.pipeline.max_items = max_items

    container = build_container(cfg, force=force, retry_failed=retry_failed)

    def _on_event(event: ProgressEvent) -> None:
        if isinstance(event, StageChanged):
            console.print(f"[bold blue]Stage:[/bold blue] {event.stage}")
        elif isinstance(event, CountersUpdated):
            console.print(
                f"  extracted={event.extracted} transformed={event.transformed} "
                f"downloaded={event.downloaded} failed={event.failed} skipped={event.skipped}"
            )
        elif isinstance(event, ErrorEvent):
            console.print(f"[red]Error ({event.component}):[/red] {event.message}")
        elif isinstance(event, Completed):
            _print_summary(event)

    container.progress_bus.subscribe(_on_event)

    runner = ETLRunner(container)
    runner.run(dry_run=dry_run)


def _print_summary(event: Completed) -> None:
    info = event.run_info
    table = Table(title="Run Summary")
    table.add_column("Metric", style="bold")
    table.add_column("Value")
    for key, value in info.counts.items():
        table.add_row(key, str(value))
    table.add_row("run_id", str(info.run_id))
    table.add_row("duration", str((info.ended_at - info.started_at) if info.ended_at else "n/a"))  # type: ignore[arg-type]
    console.print(table)
    if info.failures:
        console.print(f"[yellow]{len(info.failures)} failures recorded.[/yellow]")


@seed_app.command("validate-config")
def validate_config(
    config: Annotated[Path, typer.Option("--config", help="Path to YAML config file")],
) -> None:
    """Validate a YAML pipeline config."""
    try:
        load_config(config)
        console.print("[green]Valid[/green]")
    except ConfigError as exc:
        console.print(f"[red]Invalid:[/red] {exc}")
        raise typer.Exit(code=1) from exc


@seed_app.command("status")
def status(
    db: Annotated[Path, typer.Option("--db", help="SQLite DB path")] = Path(
        "./metadata/qdarchive.sqlite"
    ),
) -> None:
    """Show dataset and asset counts from the metadata DB."""
    if not db.exists():
        console.print(f"[yellow]Database not found:[/yellow] {db}")
        raise typer.Exit(code=1)

    conn = sqlite3.connect(db)
    try:
        table = Table(title="Status")
        table.add_column("Metric", style="bold")
        table.add_column("Count")

        row = conn.execute("SELECT COUNT(*) FROM datasets").fetchone()
        table.add_row("Datasets", str(row[0] if row else 0))

        row = conn.execute("SELECT COUNT(*) FROM assets").fetchone()
        table.add_row("Total assets", str(row[0] if row else 0))

        for status_val in ("SUCCESS", "FAILED", "SKIPPED", "RESUMABLE"):
            row = conn.execute(
                "SELECT COUNT(*) FROM assets WHERE download_status = ?", (status_val,)
            ).fetchone()
            table.add_row(f"Assets ({status_val})", str(row[0] if row else 0))

        console.print(table)
    finally:
        conn.close()


@seed_app.command("export")
def export_data(
    format: Annotated[str, typer.Option("--format", help="Export format: csv or excel")] = "csv",
    out: Annotated[Path, typer.Option("--out", help="Output path")] = Path("./export"),
    db: Annotated[Path, typer.Option("--db", help="SQLite DB path")] = Path(
        "./metadata/qdarchive.sqlite"
    ),
) -> None:
    """Export datasets and assets from the metadata DB."""
    if not db.exists():
        console.print(f"[yellow]Database not found:[/yellow] {db}")
        raise typer.Exit(code=1)

    import pandas as pd

    conn = sqlite3.connect(db)
    try:
        datasets_df = pd.read_sql_query("SELECT * FROM datasets", conn)
        assets_df = pd.read_sql_query("SELECT * FROM assets", conn)
    finally:
        conn.close()

    out.parent.mkdir(parents=True, exist_ok=True)

    if format == "csv":
        datasets_path = out.with_suffix(".datasets.csv") if out.suffix else out / "datasets.csv"
        assets_path = out.with_suffix(".assets.csv") if out.suffix else out / "assets.csv"
        datasets_path.parent.mkdir(parents=True, exist_ok=True)
        datasets_df.to_csv(datasets_path, index=False)
        assets_df.to_csv(assets_path, index=False)
        console.print(f"[green]Exported to {datasets_path} and {assets_path}[/green]")
    elif format == "excel":
        excel_path = out.with_suffix(".xlsx") if not str(out).endswith(".xlsx") else out
        with pd.ExcelWriter(excel_path) as writer:
            datasets_df.to_excel(writer, sheet_name="datasets", index=False)
            assets_df.to_excel(writer, sheet_name="assets", index=False)
        console.print(f"[green]Exported to {excel_path}[/green]")
    else:
        console.print(f"[red]Unknown format:[/red] {format}")
        raise typer.Exit(code=1)
