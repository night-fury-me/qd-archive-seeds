from __future__ import annotations

import logging
import queue
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler

from rich.logging import RichHandler


@dataclass(slots=True)
class UILogQueueHandler(logging.Handler):
    log_queue: "queue.Queue[str]"

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            self.log_queue.put_nowait(msg)
        except Exception:  # pragma: no cover
            self.handleError(record)


def build_console_handler(level: int) -> logging.Handler:
    handler = RichHandler(rich_tracebacks=True, markup=True, show_time=False, show_level=False)
    handler.setLevel(level)
    return handler


def build_file_handler(path: str, level: int, max_bytes: int, backups: int) -> logging.Handler:
    handler = RotatingFileHandler(path, maxBytes=max_bytes, backupCount=backups)
    handler.setLevel(level)
    return handler


def build_queue_handler(log_queue: "queue.Queue[str]") -> UILogQueueHandler:
    handler = UILogQueueHandler(log_queue=log_queue)
    return handler
