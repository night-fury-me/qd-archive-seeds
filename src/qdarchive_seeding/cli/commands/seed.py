from __future__ import annotations

import asyncio
import contextlib
import logging
import sqlite3
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskID, TextColumn, TimeElapsedColumn
from rich.table import Table

from qdarchive_seeding.app.config_loader import load_config
from qdarchive_seeding.app.container import build_container
from qdarchive_seeding.app.progress import (
    AssetDownloadProgress,
    AssetDownloadStarted,
    AssetDownloadUpdate,
    Completed,
    CountersUpdated,
    DateSliceProgress,
    ErrorEvent,
    MetadataCollected,
    PageProgress,
    ProgressEvent,
    QueryProgress,
    StageChanged,
)
from qdarchive_seeding.app.runner import DownloadDecision, ETLRunner
from qdarchive_seeding.core.exceptions import ConfigError

seed_app = typer.Typer(help="Seed pipeline commands.")
console = Console()

_STAGE_LABELS: dict[str, str] = {
    "metadata_collection": "Collecting metadata",
    "download": "Downloading assets",
    "done": "Done",
}


_LEVEL_STYLES: dict[str, str] = {
    "DEBUG": "dim",
    "INFO": "cyan",
    "WARNING": "yellow",
    "ERROR": "bold red",
    "CRITICAL": "bold white on red",
}


class _ProgressConsoleHandler(logging.Handler):
    """Logging handler that prints above progress bars via progress.console.log().

    Works like tqdm.write — the progress bars stay pinned at the bottom
    while log messages scroll above them.
    """

    def __init__(self, progress: Progress, level: int = logging.NOTSET) -> None:
        super().__init__(level)
        self._progress = progress

    def emit(self, record: logging.LogRecord) -> None:
        try:
            style = _LEVEL_STYLES.get(record.levelname, "")
            component = getattr(record, "component", record.name.rsplit(".", 1)[-1])
            msg = record.getMessage()
            self._progress.console.log(
                f"[{style}]{record.levelname:<8}[/{style}] [dim]{component}[/dim] │ {msg}",
                markup=True,
            )
        except Exception:
            self.handleError(record)


class CliProgressDisplay:
    """Progress display with bars for metadata queries, pages, and downloads."""

    def __init__(self) -> None:
        self._progress: Progress | None = None
        # Metadata phase task IDs
        self._query_id: TaskID | None = None
        self._slice_id: TaskID | None = None
        self._page_id: TaskID | None = None
        # Download phase task IDs
        self._overall_id: TaskID | None = None
        self._file_tasks: dict[str, TaskID] = {}  # asset_url -> task_id
        self._file_names: dict[str, str] = {}  # asset_url -> display name
        self._denied_id: TaskID | None = None
        self._total_assets: int = 0
        self._completed_assets: int = 0

    def __call__(self, event: ProgressEvent) -> None:
        if isinstance(event, StageChanged):
            self._on_stage(event)
        elif isinstance(event, QueryProgress):
            self._on_query_progress(event)
        elif isinstance(event, DateSliceProgress):
            self._on_date_slice_progress(event)
        elif isinstance(event, PageProgress):
            self._on_page_progress(event)
        elif isinstance(event, CountersUpdated):
            self._on_counters(event)
        elif isinstance(event, AssetDownloadStarted):
            self._on_download_started(event)
        elif isinstance(event, AssetDownloadProgress):
            self._on_stream_progress(event)
        elif isinstance(event, AssetDownloadUpdate):
            self._on_asset_download(event)
        elif isinstance(event, MetadataCollected):
            self._on_metadata_collected(event)
        elif isinstance(event, ErrorEvent):
            self._on_error(event)
        elif isinstance(event, Completed):
            self._stop_progress()
            _print_summary(event)

    def _on_stage(self, event: StageChanged) -> None:
        label = _STAGE_LABELS.get(event.stage, event.stage)
        if event.stage == "metadata_collection":
            self._stop_progress()
            self._progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("{task.completed}/{task.total}"),
                TimeElapsedColumn(),
                console=console,
            )
            self._query_id = self._progress.add_task("Queries: starting...", total=0, completed=0)
            self._slice_id = self._progress.add_task(
                "Date slices: n/a", total=0, completed=0, visible=False
            )
            self._page_id = self._progress.add_task("Pages: waiting...", total=0, completed=0)
            self._progress.start()
            self._suppress_console_logs()
        elif event.stage == "download":
            self._start_download_progress(label)
        elif event.stage == "done":
            self._stop_progress()
            console.print(f"[bold green]{label}[/bold green]")
        else:
            self._stop_progress()

    def _on_query_progress(self, event: QueryProgress) -> None:
        if self._progress is None or self._query_id is None:
            return
        label = f"Query {event.current_query}/{event.total_queries}: {event.query_label}"
        self._progress.update(
            self._query_id,
            description=label,
            completed=event.current_query,
            total=event.total_queries,
        )
        # Reset slice and page bars for new query
        if self._slice_id is not None:
            self._progress.reset(self._slice_id)
            self._progress.update(self._slice_id, visible=False)
        if self._page_id is not None:
            self._progress.reset(self._page_id)
            self._progress.update(self._page_id, description="Pages", total=None)

    def _on_date_slice_progress(self, event: DateSliceProgress) -> None:
        if self._progress is None or self._slice_id is None:
            return
        label = f"Date slice {event.current_slice}/{event.total_slices}: {event.slice_label}"
        self._progress.update(
            self._slice_id,
            description=label,
            completed=event.current_slice,
            total=event.total_slices,
            visible=True,
        )
        # Reset page bar for new slice
        if self._page_id is not None:
            self._progress.reset(self._page_id)
            self._progress.update(self._page_id, description="Pages", total=None)

    def _on_error(self, event: ErrorEvent) -> None:
        """Print errors through the progress bar's console to avoid corruption."""
        if self._progress is not None:
            self._progress.console.log(
                f"[bold red] ERROR   [/bold red] [dim]{event.component}[/dim] │ {event.message}"
            )

    def _on_page_progress(self, event: PageProgress) -> None:
        if self._progress is None or self._page_id is None:
            return
        page_size = (
            int(self._progress.tasks[self._query_id].fields.get("page_size", 100))
            if self._query_id is not None
            else 100
        )
        # Estimate total pages from API total_hits
        if event.total_hits > 0:
            est_pages = (event.total_hits + page_size - 1) // page_size
            # Cap display at 100 pages (API limit for Zenodo)
            est_pages = min(est_pages, 100)
        else:
            est_pages = None
        self._progress.update(
            self._page_id,
            description=f"Page {event.current_page}" + (f"/{est_pages}" if est_pages else ""),
            completed=event.current_page,
            total=est_pages,
        )

    def _on_counters(self, event: CountersUpdated) -> None:
        if event.total_assets > 0:
            self._total_assets = event.total_assets
            if self._progress is not None and self._overall_id is not None:
                self._progress.update(
                    self._overall_id,
                    total=self._total_assets,
                )
        # Update completed count — include skipped so already-downloaded assets
        # immediately advance the progress bar instead of leaving a gap.
        if self._progress is not None and self._overall_id is not None:
            completed = event.downloaded + event.failed + event.skipped
            if self._total_assets > 0:
                completed = min(completed, self._total_assets)
            self._completed_assets = completed
            self._progress.update(
                self._overall_id,
                description=f"Downloading assets ({completed}/{self._total_assets})",
                completed=completed,
            )
        if event.access_denied > 0 and self._progress is not None and self._denied_id is not None:
            # Show as text-only — no progress bar needed for a simple counter
            self._progress.update(
                self._denied_id,
                description=(
                    f"[yellow]Errors: {event.access_denied} files"
                    " (access denied / network)[/yellow]"
                ),
                visible=True,
                total=0,
                completed=0,
            )

    def _on_metadata_collected(self, event: MetadataCollected) -> None:
        self._stop_progress()
        table = Table(title="Metadata Collection Summary")
        table.add_column("Metric", style="bold")
        table.add_column("Value")
        table.add_row("Total unique datasets", str(event.total_projects))
        table.add_row("Total files", str(event.total_files))
        table.add_row("Estimated total size", _format_size(event.total_size_bytes))
        console.print(table)

    def _on_download_started(self, event: AssetDownloadStarted) -> None:
        if self._progress is None:
            return
        filename = event.filename
        if len(filename) > 40:
            filename = filename[:37] + "..."
        task_id = self._progress.add_task(f"[dim]{filename}[/dim]", total=None, completed=0)
        self._file_tasks[event.asset_url] = task_id
        self._file_names[event.asset_url] = filename

    def _on_stream_progress(self, event: AssetDownloadProgress) -> None:
        if self._progress is None:
            return
        task_id = self._file_tasks.get(event.asset_url)
        if task_id is None:
            return
        if event.total_bytes is not None:
            self._progress.update(
                task_id,
                completed=event.bytes_downloaded,
                total=event.total_bytes,
            )
        else:
            self._progress.update(
                task_id,
                completed=event.bytes_downloaded,
            )

    def _on_asset_download(self, event: AssetDownloadUpdate) -> None:
        if self._progress is None:
            return
        task_id = self._file_tasks.pop(event.asset_url, None)
        filename = self._file_names.pop(event.asset_url, None)
        if task_id is not None:
            self._progress.remove_task(task_id)
        if filename:
            size = _format_size(event.bytes_downloaded) if event.bytes_downloaded else ""
            status_style = "green" if event.status == "SUCCESS" else "red"
            if event.status != "SUCCESS":
                reason = event.error_message or "unknown error"
                if len(reason) > 80:
                    reason = reason[:77] + "..."
                self._progress.console.print(
                    f"  [{status_style}]{event.status}[/{status_style}] {filename}"
                    + (f" ({size})" if size else "")
                    + f"\n    [dim]{event.asset_url}[/dim]"
                    + f"\n    [dim italic]{reason}[/dim italic]"
                )
            else:
                self._progress.console.print(
                    f"  [{status_style}]{event.status}[/{status_style}] {filename}"
                    + (f" ({size})" if size else "")
                )

    def _start_download_progress(self, label: str) -> None:
        self._stop_progress()
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=console,
        )
        self._overall_id = self._progress.add_task(label, total=self._total_assets or None)
        self._denied_id = self._progress.add_task("Errors: 0 files", total=0, visible=False)
        self._file_tasks = {}
        self._file_names = {}
        self._progress.start()
        self._suppress_console_logs()

    def _suppress_console_logs(self) -> None:
        """Replace RichHandlers with a proxy that prints via progress.console.log().

        This ensures logs appear above the progress bars (like tqdm.write)
        instead of corrupting them.
        """
        self._suppressed: list[tuple[logging.Logger, logging.Handler, int]] = []
        self._proxy_handlers: list[tuple[logging.Logger, logging.Handler]] = []
        if self._progress is None:
            return
        progress_ref = self._progress

        # Scan root + all registered loggers (including the app logger)
        all_loggers: list[logging.Logger] = [logging.root]
        for name, obj in logging.root.manager.loggerDict.items():
            if isinstance(obj, logging.Logger):
                all_loggers.append(obj)
            else:
                # PlaceHolder — resolve to actual logger
                all_loggers.append(logging.getLogger(name))

        for lgr in all_loggers:
            for handler in list(lgr.handlers):
                if isinstance(handler, logging.Handler) and hasattr(handler, "console"):
                    # Save original level before suppressing
                    orig_level = handler.level
                    self._suppressed.append((lgr, handler, orig_level))
                    handler.setLevel(logging.CRITICAL + 1)

                    # Install proxy with the original level
                    proxy = _ProgressConsoleHandler(progress_ref, orig_level)
                    for f in handler.filters:
                        proxy.addFilter(f)
                    lgr.addHandler(proxy)
                    self._proxy_handlers.append((lgr, proxy))

    def _restore_console_logs(self) -> None:
        """Remove proxies and restore original RichHandler levels."""
        for lgr, proxy in getattr(self, "_proxy_handlers", []):
            lgr.removeHandler(proxy)
        self._proxy_handlers = []
        for _lgr, handler, level in getattr(self, "_suppressed", []):
            handler.setLevel(level)
        self._suppressed = []

    def _stop_progress(self) -> None:
        if self._progress is not None:
            # Clean up any orphaned file task bars
            for task_id in self._file_tasks.values():
                self._progress.remove_task(task_id)
            self._file_tasks.clear()
            self._file_names.clear()
            self._progress.stop()
            self._progress = None
            self._query_id = None
            self._slice_id = None
            self._page_id = None
            self._overall_id = None
        self._restore_console_logs()


def _format_size(size_bytes: int) -> str:
    """Format byte count as human-readable string."""
    if size_bytes <= 0:
        return "unknown"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024  # type: ignore[assignment]
    return f"{size_bytes:.1f} PB"


def _prompt_download_decision(
    total_projects: int, total_files: int, total_size_bytes: int
) -> DownloadDecision:
    """Prompt the user for a download decision after metadata collection.

    Called synchronously by the runner between Phase 1 and Phase 2.
    Uses plain print/input to avoid Rich/typer stdin interference.
    """
    import sys

    size_str = _format_size(total_size_bytes)
    print(flush=True)
    print(f"Found {total_projects} datasets, {total_files} files, ~{size_str}.")
    print("Download options:")
    print("  [1] Download all datasets")
    print("  [2] Download a percentage of datasets")
    print("  [3] Download an exact number of datasets")
    print("  [4] Skip download (metadata only)")
    print(flush=True)
    sys.stdout.flush()

    try:
        raw = input("Choose an option [1]: ").strip()
        choice = int(raw) if raw else 1
    except (ValueError, EOFError):
        choice = 1

    if choice == 1:
        return DownloadDecision(download_all=True, percentage=100)
    elif choice == 2:
        try:
            raw = input(
                f"Percentage of {total_projects} datasets to download (1-100) [100]: "
            ).strip()
            pct = int(raw) if raw else 100
        except (ValueError, EOFError):
            pct = 100
        pct = max(1, min(100, pct))
        return DownloadDecision(download_all=False, percentage=pct)
    elif choice == 3:
        try:
            raw = input(
                f"Number of datasets to download (1-{total_projects}) [{total_projects}]: "
            ).strip()
            count = int(raw) if raw else total_projects
        except (ValueError, EOFError):
            count = total_projects
        count = max(1, min(total_projects, count))
        return DownloadDecision(download_all=False, exact_count=count)
    else:
        return DownloadDecision(download_all=False, percentage=0)


def _prompt_icpsr_login(icpsr_count: int) -> bool:
    """Notify the user about ICPSR browser login and give time to log in.

    Non-blocking: always returns True to proceed with downloads.
    The user can log in now or just press Enter to continue without logging in.
    """
    from rich.panel import Panel
    from rich.text import Text

    body = Text()
    body.append(f"{icpsr_count} files", style="bold yellow")
    body.append(" are hosted on ICPSR and require an active browser login session.\n\n")
    body.append("Login is optional", style="bold")
    body.append(" — the pipeline will continue either way.\n")
    body.append("If you are not logged in, ICPSR files will be ")
    body.append("skipped or won't be able to download", style="bold red")
    body.append("; all other downloads proceed normally.\n\n")
    body.append("To log in:\n")
    body.append("  1. Open a Chromium-based browser on this machine\n")
    body.append("  2. Log in to both:\n")
    body.append("     - ")
    body.append("https://www.icpsr.umich.edu/mydata", style="bold underline cyan")
    body.append("\n     - ")
    body.append("https://www.openicpsr.org/openicpsr/", style="bold underline cyan")
    body.append("\n  3. Sign in via institutional SSO\n")
    body.append("  4. Come back here and press Enter\n")

    console.print()
    console.print(Panel(body, title="ICPSR Browser Login", border_style="yellow", expand=False))

    with contextlib.suppress(EOFError):
        console.input("[dim]Press Enter to continue...[/dim] ")

    return True


def _prompt_icpsr_terms_url(url: str) -> None:
    """Show the ICPSR URL that requires manual agreement and wait for Enter."""
    console.print(
        f"\n[yellow]ICPSR manual agreement required:[/yellow]\n"
        f"  [bold underline cyan]{url}[/bold underline cyan]\n"
        f"[dim]Accept the terms in your browser, then press Enter to retry "
        f"(or just press Enter to skip).[/dim]"
    )
    with contextlib.suppress(EOFError):
        input()


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

    display = CliProgressDisplay()
    container.progress_bus.subscribe(display)

    runner = ETLRunner(container)
    asyncio.run(
        runner.run(
            dry_run=dry_run,
            no_confirm=no_confirm,
            metadata_only=metadata_only,
            fresh_extract=fresh_extract,
            confirm_callback=_prompt_download_decision if not no_confirm else None,
            icpsr_confirm_callback=_prompt_icpsr_login if not no_confirm else None,
            icpsr_terms_url_callback=_prompt_icpsr_terms_url if not no_confirm else None,
        )
    )


def _print_summary(event: Completed) -> None:
    info = event.run_info
    table = Table(title="Run Summary")
    table.add_column("Metric", style="bold")
    table.add_column("Value")
    for key, value in info.counts.items():
        table.add_row(key, str(value))
    table.add_row("run_id", str(info.run_id))
    duration = str(info.ended_at - info.started_at) if info.ended_at else "n/a"
    table.add_row("duration", duration)
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
