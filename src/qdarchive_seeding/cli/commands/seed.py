from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from qdarchive_seeding.app.config_loader import load_config
from qdarchive_seeding.app.container import build_container
from qdarchive_seeding.app.runner import ETLRunner
from qdarchive_seeding.cli.display import CliProgressDisplay
from qdarchive_seeding.cli.prompts import (
    prompt_download_decision,
    prompt_icpsr_login,
    prompt_icpsr_terms_url,
)
from qdarchive_seeding.core.exceptions import ConfigError

seed_app = typer.Typer(help="Seed pipeline commands.")
console = Console()


@seed_app.command("run")
def run_pipeline(
    config: Annotated[Path, typer.Option("--config", help="Path to YAML config file")],
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Run without downloading")] = False,
    fresh_download: Annotated[
        bool, typer.Option("--fresh-download", help="Re-download all files, ignoring prior status")
    ] = False,
    max_items: Annotated[int | None, typer.Option("--max-items", help="Override max items")] = None,
    retry_failed: Annotated[
        bool, typer.Option("--retry-failed", help="Retry failed downloads")
    ] = False,
    no_confirm: Annotated[
        bool, typer.Option("--no-confirm", help="Skip download confirmation prompt")
    ] = False,
    metadata_only: Annotated[
        bool, typer.Option("--metadata-only", help="Only collect metadata, skip downloads")
    ] = False,
    fresh_extract: Annotated[
        bool,
        typer.Option(
            "--fresh-extract",
            help="Clear checkpoint and re-extract all queries from scratch",
        ),
    ] = False,
) -> None:
    """Run a seeding pipeline from a YAML config."""
    try:
        cfg = load_config(config)
    except ConfigError as exc:
        console.print(f"[red]Config error:[/red] {exc}")
        raise typer.Exit(code=1) from exc

    if max_items is not None:
        cfg.pipeline.max_items = max_items

    container = build_container(cfg, fresh_download=fresh_download, retry_failed=retry_failed)

    if fresh_extract:
        container.checkpoint.clear()
        console.print("[yellow]Checkpoint cleared — extracting all queries from scratch.[/yellow]")

    display = CliProgressDisplay(console)
    container.progress_bus.subscribe(display)

    runner = ETLRunner(container)

    # Wrap prompt functions to bind the console instance
    def _icpsr_login_cb(icpsr_count: int) -> bool:
        return prompt_icpsr_login(console, icpsr_count)

    def _icpsr_terms_cb(url: str) -> None:
        prompt_icpsr_terms_url(console, url)

    # asyncio.run() creates a new event loop; it will raise RuntimeError if
    # called from within an already-running loop (e.g. Jupyter).  In that
    # scenario the caller is responsible for awaiting runner.run() directly.
    asyncio.run(
        runner.run(
            dry_run=dry_run,
            no_confirm=no_confirm,
            metadata_only=metadata_only,
            fresh_extract=fresh_extract,
            confirm_callback=prompt_download_decision if not no_confirm else None,
            icpsr_confirm_callback=_icpsr_login_cb if not no_confirm else None,
            icpsr_terms_url_callback=_icpsr_terms_cb if not no_confirm else None,
        )
    )


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
    """Show project and file counts from the metadata DB."""
    if not db.exists():
        console.print(f"[yellow]Database not found:[/yellow] {db}")
        raise typer.Exit(code=1)

    conn = sqlite3.connect(db)
    try:
        table = Table(title="Status")
        table.add_column("Metric", style="bold")
        table.add_column("Count")

        row = conn.execute("SELECT COUNT(*) FROM projects").fetchone()
        table.add_row("Projects", str(row[0] if row else 0))

        row = conn.execute("SELECT COUNT(*) FROM files").fetchone()
        table.add_row("Total files", str(row[0] if row else 0))

        for status_val in ("SUCCESS", "FAILED", "SKIPPED", "UNKNOWN"):
            row = conn.execute(
                "SELECT COUNT(*) FROM files WHERE status = ?", (status_val,)
            ).fetchone()
            table.add_row(f"Files ({status_val})", str(row[0] if row else 0))

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
    """Export projects and files from the metadata DB."""
    if not db.exists():
        console.print(f"[yellow]Database not found:[/yellow] {db}")
        raise typer.Exit(code=1)

    import pandas as pd  # type: ignore[import-untyped]  # no stubs available

    conn = sqlite3.connect(db)
    try:
        projects_df = pd.read_sql_query("SELECT * FROM projects", conn)
        files_df = pd.read_sql_query("SELECT * FROM files", conn)
    finally:
        conn.close()

    out.parent.mkdir(parents=True, exist_ok=True)

    if format == "csv":
        projects_path = out.with_suffix(".projects.csv") if out.suffix else out / "projects.csv"
        files_path = out.with_suffix(".files.csv") if out.suffix else out / "files.csv"
        projects_path.parent.mkdir(parents=True, exist_ok=True)
        projects_df.to_csv(projects_path, index=False)
        files_df.to_csv(files_path, index=False)
        console.print(f"[green]Exported to {projects_path} and {files_path}[/green]")
    elif format == "excel":
        excel_path = out.with_suffix(".xlsx") if not str(out).endswith(".xlsx") else out
        with pd.ExcelWriter(excel_path) as writer:
            projects_df.to_excel(writer, sheet_name="projects", index=False)
            files_df.to_excel(writer, sheet_name="files", index=False)
        console.print(f"[green]Exported to {excel_path}[/green]")
    else:
        console.print(f"[red]Unknown format:[/red] {format}")
        raise typer.Exit(code=1)
