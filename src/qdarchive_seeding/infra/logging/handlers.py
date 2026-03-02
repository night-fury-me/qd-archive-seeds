from __future__ import annotations

import logging
import queue
from logging.handlers import RotatingFileHandler
from pathlib import Path

from rich.logging import RichHandler


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
    handler = RichHandler(rich_tracebacks=True, markup=True, show_time=False, show_level=False)
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
