from __future__ import annotations

import queue

from textual.widgets import RichLog


class LogViewerWidget(RichLog):
    def __init__(
        self,
        log_queue: queue.Queue[str] | None = None,
        **kwargs: object,
    ) -> None:
        super().__init__(**kwargs)
        self._log_queue = log_queue

    def on_mount(self) -> None:
        if self._log_queue is not None:
            self.set_interval(0.1, self._drain_queue)

    def _drain_queue(self) -> None:
        if self._log_queue is None:
            return
        drained = 0
        while drained < 100:
            try:
                message = self._log_queue.get_nowait()
                self.write(message)
                drained += 1
            except queue.Empty:
                break
