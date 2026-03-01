from __future__ import annotations

from pathlib import Path

from textual.app import App

from qdarchive_seeding.tui.screens.home import HomeScreen
from qdarchive_seeding.tui.screens.run_monitor import RunMonitorScreen
from qdarchive_seeding.tui.screens.run_select import RunSelectScreen


class QDArchiveApp(App[None]):
    TITLE = "QDArchive Seeding"
    CSS_PATH = "css/theme.tcss"
    BINDINGS = [("q", "quit", "Quit")]

    SCREENS = {
        "home": HomeScreen,
        "run_select": RunSelectScreen,
        "run_monitor": RunMonitorScreen,
    }

    def on_mount(self) -> None:
        self.push_screen("home")


def main() -> None:
    app = QDArchiveApp()
    app.run()


if __name__ == "__main__":
    main()
