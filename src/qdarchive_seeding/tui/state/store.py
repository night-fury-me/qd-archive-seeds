from __future__ import annotations

from pathlib import Path

from textual.reactive import reactive
from textual.widget import Widget


class AppState(Widget):
    config_path: reactive[str] = reactive("")
    last_run_id: reactive[str] = reactive("")
    theme: reactive[str] = reactive("dark")
    downloads_root: reactive[str] = reactive("./downloads")
    metadata_db: reactive[str] = reactive("./metadata/qdarchive.sqlite")
    runs_dir: reactive[str] = reactive("./runs")
