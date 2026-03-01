from __future__ import annotations

from pathlib import Path

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import DirectoryTree, Footer, Header, Static


class BrowseDownloadsScreen(Screen[None]):
    BINDINGS = [("escape", "pop_screen", "Back")]

    def __init__(self, downloads_root: Path = Path("downloads")) -> None:
        super().__init__()
        self._downloads_root = downloads_root

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal(id="browse"):
            if self._downloads_root.exists():
                yield DirectoryTree(str(self._downloads_root), id="browse-tree")
            else:
                yield Static(
                    f"[yellow]Downloads directory not found: {self._downloads_root}[/yellow]",
                    id="browse-tree",
                )
            with Vertical(id="browse-detail"):
                yield Static("[bold]File Details[/bold]", id="detail-title")
                yield Static("Select a file to view details", id="detail-content")
        yield Footer()

    def on_directory_tree_file_selected(self, event: DirectoryTree.FileSelected) -> None:
        path = Path(event.path)
        try:
            stat = path.stat()
            info = (
                f"[bold]{path.name}[/bold]\n"
                f"Path: {path}\n"
                f"Size: {stat.st_size:,} bytes\n"
                f"Modified: {stat.st_mtime}"
            )
        except OSError as exc:
            info = f"[red]Error reading file: {exc}[/red]"
        self.query_one("#detail-content", Static).update(info)

    def action_pop_screen(self) -> None:
        self.app.pop_screen()
