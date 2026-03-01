from __future__ import annotations

from textual.app import App

from qdarchive_seeding.tui.screens.browse_downloads import BrowseDownloadsScreen
from qdarchive_seeding.tui.screens.config_editor import ConfigEditorScreen
from qdarchive_seeding.tui.screens.config_validate import ConfigValidateScreen
from qdarchive_seeding.tui.screens.config_wizard import ConfigWizardScreen
from qdarchive_seeding.tui.screens.home import HomeScreen
from qdarchive_seeding.tui.screens.run_history import RunHistoryScreen
from qdarchive_seeding.tui.screens.run_monitor import RunMonitorScreen
from qdarchive_seeding.tui.screens.run_select import RunSelectScreen
from qdarchive_seeding.tui.screens.settings import SettingsScreen


class QDArchiveApp(App[None]):
    TITLE = "QDArchive Seeding"
    CSS_PATH = "css/theme.tcss"
    BINDINGS = [("q", "quit", "Quit")]

    SCREENS = {
        "home": HomeScreen,
        "run_select": RunSelectScreen,
        "run_monitor": RunMonitorScreen,
        "config_wizard": ConfigWizardScreen,
        "config_editor": ConfigEditorScreen,
        "config_validate": ConfigValidateScreen,
        "run_history": RunHistoryScreen,
        "browse_downloads": BrowseDownloadsScreen,
        "settings": SettingsScreen,
    }

    def on_mount(self) -> None:
        self.push_screen("home")


def main() -> None:
    app = QDArchiveApp()
    app.run()


if __name__ == "__main__":
    main()
