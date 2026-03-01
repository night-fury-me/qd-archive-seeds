from __future__ import annotations

from textual.reactive import reactive
from textual.widgets import Static


class ProgressPanel(Static):
    stage: reactive[str] = reactive("idle")
    extracted: reactive[int] = reactive(0)
    transformed: reactive[int] = reactive(0)
    downloaded: reactive[int] = reactive(0)
    failed: reactive[int] = reactive(0)
    skipped: reactive[int] = reactive(0)

    def render(self) -> str:
        return (
            f"[bold]Stage:[/bold] {self.stage}\n"
            f"Extracted: {self.extracted}  Transformed: {self.transformed}\n"
            f"Downloaded: {self.downloaded}  Failed: {self.failed}  Skipped: {self.skipped}"
        )
