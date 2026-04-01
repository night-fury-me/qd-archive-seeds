from __future__ import annotations

import logging
from collections import deque
from collections.abc import Callable

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskID, TextColumn, TimeElapsedColumn
from rich.table import Table
from rich.text import Text

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


# Module-level ref for pause/resume around prompts (shared with prompts module)
_active_live: Live | None = None


def get_active_live() -> Live | None:
    """Return the currently active Live display, if any."""
    return _active_live


def set_active_live(live: Live | None) -> None:
    """Set the module-level active Live display reference."""
    global _active_live  # noqa: PLW0603
    _active_live = live


class _ProgressConsoleHandler(logging.Handler):
    """Logging handler that routes messages to a callback or progress.console.log().

    During download phase with Live display, routes to the log panel callback.
    During metadata phase, routes to progress.console.log().
    """

    def __init__(
        self,
        progress: Progress,
        level: int = logging.NOTSET,
        log_callback: Callable[[str], None] | None = None,
    ) -> None:
        super().__init__(level)
        self._progress = progress
        self._log_callback = log_callback

    def emit(self, record: logging.LogRecord) -> None:
        try:
            style = _LEVEL_STYLES.get(record.levelname, "")
            component = getattr(record, "component", record.name.rsplit(".", 1)[-1])
            msg = record.getMessage()
            line = f"[{style}]{record.levelname:<8}[/{style}] [dim]{component}[/dim] │ {msg}"
            if self._log_callback is not None:
                self._log_callback(line)
                return
            self._progress.console.log(
                line,
                markup=True,
            )
        except Exception:
            self.handleError(record)


class CliProgressDisplay:
    """Progress display with bars for metadata queries, pages, and downloads."""

    _LOG_PANEL_LINES = 10

    def __init__(self, console: Console) -> None:
        self._console = console
        self._progress: Progress | None = None
        self._live: Live | None = None
        # Metadata phase task IDs
        self._query_id: TaskID | None = None
        self._slice_id: TaskID | None = None
        self._page_id: TaskID | None = None
        # Download phase task IDs
        self._overall_id: TaskID | None = None
        self._denied_id: TaskID | None = None
        # Fixed pool of file slots (avoids jumping when bars are added/removed)
        self._max_file_slots = 5
        self._file_slot_ids: list[TaskID] = []  # pre-created task IDs
        self._slot_to_url: dict[int, str] = {}  # slot_index -> asset_url
        self._url_to_slot: dict[str, int] = {}  # asset_url -> slot_index
        self._file_names: dict[str, str] = {}  # asset_url -> display name
        self._total_assets: int = 0
        # Log panel (download phase only)
        self._log_lines: deque[str] = deque(maxlen=self._LOG_PANEL_LINES)
        self._completed_assets: int = 0
        # Console log suppression state (populated by _suppress_console_logs)
        self._suppressed: list[tuple[logging.Logger, logging.Handler, int]] = []
        self._proxy_handlers: list[tuple[logging.Logger, logging.Handler]] = []

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
            _print_summary(self._console, event)

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
            )
            self._query_id = self._progress.add_task("Queries: starting...", total=0, completed=0)
            self._slice_id = self._progress.add_task(
                "Date slices: n/a", total=0, completed=0, visible=False
            )
            self._page_id = self._progress.add_task("Pages: waiting...", total=0, completed=0)
            self._log_lines.clear()
            self._live = Live(self._build_layout(), console=self._console, refresh_per_second=4)
            self._live.start()
            set_active_live(self._live)
            self._suppress_console_logs()
        elif event.stage == "download":
            self._start_download_progress(label)
        elif event.stage == "done":
            self._stop_progress()
            self._console.print(f"[bold green]{label}[/bold green]")
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
        """Log errors to the download log panel."""
        msg = event.message
        if len(msg) > 80:
            msg = msg[:77] + "..."
        self._log_to_panel(f"[bold red]ERROR[/bold red] [dim]{event.component}[/dim] {msg}")

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
        from qdarchive_seeding.cli.prompts import format_size

        self._stop_progress()
        table = Table(title="Metadata Collection Summary")
        table.add_column("Metric", style="bold")
        table.add_column("Value")
        table.add_row("Total unique datasets", str(event.total_projects))
        table.add_row("Total files", str(event.total_files))
        table.add_row("Estimated total size", format_size(event.total_size_bytes))
        self._console.print(table)

    def _on_download_started(self, event: AssetDownloadStarted) -> None:
        if self._progress is None:
            return
        filename = event.filename
        if len(filename) > 40:
            filename = filename[:37] + "..."
        self._file_names[event.asset_url] = filename
        # Find a free slot
        for i in range(self._max_file_slots):
            if i not in self._slot_to_url:
                self._slot_to_url[i] = event.asset_url
                self._url_to_slot[event.asset_url] = i
                self._progress.reset(self._file_slot_ids[i])
                self._progress.update(
                    self._file_slot_ids[i],
                    description=f"[dim]{filename}[/dim]",
                    total=None,
                    completed=0,
                )
                return

    def _on_stream_progress(self, event: AssetDownloadProgress) -> None:
        if self._progress is None:
            return
        slot = self._url_to_slot.get(event.asset_url)
        if slot is None:
            return
        task_id = self._file_slot_ids[slot]
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
        from qdarchive_seeding.cli.prompts import format_size

        if self._progress is None:
            return
        slot = self._url_to_slot.pop(event.asset_url, None)
        filename = self._file_names.pop(event.asset_url, None)
        if slot is not None:
            self._slot_to_url.pop(slot, None)
            self._progress.reset(self._file_slot_ids[slot])
            self._progress.update(
                self._file_slot_ids[slot],
                description="[dim]—[/dim]",
                total=0,
                completed=0,
            )
        if filename:
            size = format_size(event.bytes_downloaded) if event.bytes_downloaded else ""
            if event.status != "SUCCESS":
                reason = event.error_message or "unknown error"
                if len(reason) > 80:
                    reason = reason[:77] + "..."
                self._log_to_panel(
                    f"[red]FAILED[/red] {filename}"
                    + (f" ({size})" if size else "")
                    + f" — [dim]{reason}[/dim]"
                )
                self._log_to_panel(f"  [dim]{event.asset_url}[/dim]")
            else:
                self._log_to_panel(f"[green]OK[/green] {filename}" + (f" ({size})" if size else ""))

    def _log_to_panel(self, line: str) -> None:
        """Append a message to the fixed log panel and refresh the Live display."""
        self._log_lines.append(line)
        self._refresh_live()

    def _refresh_live(self) -> None:
        """Refresh the Live display with current layout."""
        if self._live is not None:
            self._live.update(self._build_layout())

    def _build_layout(self) -> Group:
        """Build the Live renderable: log panel on top, progress bars below."""
        lines = list(self._log_lines)
        while len(lines) < self._LOG_PANEL_LINES:
            lines.append("")
        log_text = Text.from_markup("\n".join(lines))
        log_text.no_wrap = True
        log_text.overflow = "ellipsis"
        title = "[bold]Download Log[/bold]" if self._overall_id is not None else "[bold]Log[/bold]"
        log_panel = Panel(
            log_text,
            title=title,
            border_style="dim",
            height=self._LOG_PANEL_LINES + 2,  # +2 for border
            expand=True,
        )
        return Group(log_panel, self._progress)  # type: ignore[arg-type]

    def _start_download_progress(self, label: str) -> None:
        self._stop_progress()
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
        )
        self._overall_id = self._progress.add_task(label, total=self._total_assets or None)
        self._denied_id = self._progress.add_task("Errors: 0 files", total=0, visible=False)
        self._file_slot_ids = [
            self._progress.add_task("[dim]—[/dim]", total=0, completed=0)
            for _ in range(self._max_file_slots)
        ]
        self._slot_to_url = {}
        self._url_to_slot = {}
        self._file_names = {}
        self._log_lines.clear()
        self._live = Live(self._build_layout(), console=self._console, refresh_per_second=4)
        self._live.start()
        set_active_live(self._live)
        self._suppress_console_logs()

    def _suppress_console_logs(self) -> None:
        """Replace RichHandlers with a proxy that prints via progress.console.log().

        This ensures logs appear above the progress bars (like tqdm.write)
        instead of corrupting them.
        """
        self._suppressed.clear()
        self._proxy_handlers.clear()
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
                    log_cb = self._log_to_panel if self._live is not None else None
                    proxy = _ProgressConsoleHandler(progress_ref, orig_level, log_callback=log_cb)
                    for f in handler.filters:
                        proxy.addFilter(f)
                    lgr.addHandler(proxy)
                    self._proxy_handlers.append((lgr, proxy))

    def _restore_console_logs(self) -> None:
        """Remove proxies and restore original RichHandler levels."""
        for lgr, proxy in self._proxy_handlers:
            lgr.removeHandler(proxy)
        self._proxy_handlers = []
        for _lgr, handler, level in self._suppressed:
            handler.setLevel(level)
        self._suppressed = []

    def _stop_progress(self) -> None:
        if self._live is not None:
            self._live.stop()
            self._live = None
            set_active_live(None)
        elif self._progress is not None:
            # Metadata phase uses progress.start() directly (no Live wrapper)
            self._progress.stop()
        self._progress = None
        self._slot_to_url.clear()
        self._url_to_slot.clear()
        self._file_names.clear()
        self._query_id = None
        self._slice_id = None
        self._page_id = None
        self._overall_id = None
        self._file_slot_ids = []
        self._restore_console_logs()


def _print_summary(console: Console, event: Completed) -> None:
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
