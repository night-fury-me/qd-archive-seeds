from __future__ import annotations

import contextlib

from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from qdarchive_seeding.app.download_strategy import DownloadDecision
from qdarchive_seeding.cli.display import get_active_live


def format_size(size_bytes: int) -> str:
    """Format byte count as human-readable string."""
    if size_bytes <= 0:
        return "unknown"
    size = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


def prompt_download_decision(
    total_projects: int, total_files: int, total_size_bytes: int
) -> DownloadDecision:
    """Prompt the user for a download decision after metadata collection.

    Called synchronously by the runner between Phase 1 and Phase 2.
    Uses plain print/input to avoid Rich/typer stdin interference.
    """
    import sys

    size_str = format_size(total_size_bytes)
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


def prompt_icpsr_login(console: Console, icpsr_count: int) -> bool:
    """Notify the user about ICPSR browser login and give time to log in.

    Non-blocking: always returns True to proceed with downloads.
    The user can log in now or just press Enter to continue without logging in.
    """
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

    active_live = get_active_live()
    if active_live is not None:
        active_live.stop()
    try:
        console.print()
        console.print(Panel(body, title="ICPSR Browser Login", border_style="yellow", expand=False))
        with contextlib.suppress(EOFError):
            console.input("[dim]Press Enter to continue...[/dim] ")
    finally:
        if active_live is not None:
            active_live.start()

    return True


def prompt_icpsr_terms_url(console: Console, url: str) -> None:
    """Show the ICPSR URL that requires manual agreement and wait for Enter."""
    active_live = get_active_live()
    if active_live is not None:
        active_live.stop()
    try:
        console.print(
            f"\n[yellow]ICPSR manual agreement required:[/yellow]\n"
            f"  [bold underline cyan]{url}[/bold underline cyan]\n"
            f"[dim]Accept the terms in your browser, then press Enter to retry "
            f"(or just press Enter to skip).[/dim]"
        )
        with contextlib.suppress(EOFError):
            input()
    finally:
        if active_live is not None:
            active_live.start()
