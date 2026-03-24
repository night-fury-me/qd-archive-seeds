from __future__ import annotations

import logging
import queue
from logging.handlers import RotatingFileHandler
from pathlib import Path

from rich.logging import RichHandler
from rich.text import Text

_LEVEL_STYLES: dict[str, str] = {
    "DEBUG": "dim",
    "INFO": "cyan",
    "WARNING": "yellow",
    "ERROR": "bold red",
    "CRITICAL": "bold white on red",
}


class StyledRichHandler(RichHandler):
    """RichHandler with colored level badges and dimmed metadata."""

    def get_level_text(self, record: logging.LogRecord) -> Text:
        level = record.levelname
        style = _LEVEL_STYLES.get(level, "")
        return Text(f" {level:<8}", style=style)


class UILogQueueHandler(logging.Handler):
    def __init__(self, log_queue: queue.Queue[str]) -> None:
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            self.log_queue.put_nowait(msg)
        except Exception:
            self.handleError(record)


def build_console_handler(level: int) -> logging.Handler:
    handler = StyledRichHandler(
        rich_tracebacks=True,
        markup=True,
        show_time=True,
        show_level=True,
        show_path=False,
        log_time_format="[%H:%M:%S]",
    )
    handler.setLevel(level)
    return handler


def build_file_handler(path: str, level: int, max_bytes: int, backups: int) -> logging.Handler:
    log_path = Path(path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(log_path, maxBytes=max_bytes, backupCount=backups)
    handler.setLevel(level)
    return handler


def build_queue_handler(log_queue: queue.Queue[str]) -> UILogQueueHandler:
    return UILogQueueHandler(log_queue=log_queue)
