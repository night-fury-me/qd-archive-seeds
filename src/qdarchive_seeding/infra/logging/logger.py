from __future__ import annotations

import logging
import queue
from dataclasses import dataclass

from qdarchive_seeding.app.config_models import LoggingSettings
from qdarchive_seeding.infra.logging.filters import ContextFilter
from qdarchive_seeding.infra.logging.formatters import ContextFormatter
from qdarchive_seeding.infra.logging.handlers import (
    build_console_handler,
    build_file_handler,
    build_queue_handler,
)


@dataclass(slots=True)
class LoggerBundle:
    logger: logging.Logger
    log_queue: queue.Queue[str] | None = None


def configure_logger(
    name: str,
    settings: LoggingSettings,
    *,
    run_id: str | None = None,
    pipeline_id: str | None = None,
    component: str | None = None,
    enable_queue: bool = False,
) -> LoggerBundle:
    logger = logging.getLogger(name)
    logger.setLevel(logging.getLevelName(settings.level))
    logger.propagate = False
    logger.handlers.clear()

    console_fmt = "%(asctime)s | %(levelname)s | %(component)s | %(message)s"
    file_fmt = (
        "%(asctime)s | %(levelname)s | %(component)s"
        " | run=%(run_id)s | pipeline=%(pipeline_id)s | %(message)s"
    )

    context_filter = ContextFilter(run_id=run_id, pipeline_id=pipeline_id, component=component)

    if settings.console.enabled:
        console_formatter = ContextFormatter(
            fmt=console_fmt,
            run_id=run_id,
            pipeline_id=pipeline_id,
        )
        console_handler = build_console_handler(logging.getLevelName(settings.level))
        console_handler.setFormatter(console_formatter)
        console_handler.addFilter(context_filter)
        logger.addHandler(console_handler)

    if settings.file.enabled:
        if not settings.file.path:
            raise ValueError("File logging enabled but no path provided")
        file_formatter = ContextFormatter(
            fmt=file_fmt,
            run_id=run_id,
            pipeline_id=pipeline_id,
        )
        file_handler = build_file_handler(
            settings.file.path, logging.getLevelName(settings.level), 5_000_000, 3
        )
        file_handler.setFormatter(file_formatter)
        file_handler.addFilter(context_filter)
        logger.addHandler(file_handler)

    log_queue: queue.Queue[str] | None = None
    if enable_queue:
        log_queue = queue.Queue(maxsize=10_000)
        queue_formatter = ContextFormatter(
            fmt=file_fmt,
            run_id=run_id,
            pipeline_id=pipeline_id,
        )
        queue_handler = build_queue_handler(log_queue)
        queue_handler.setFormatter(queue_formatter)
        queue_handler.addFilter(context_filter)
        logger.addHandler(queue_handler)

    return LoggerBundle(logger=logger, log_queue=log_queue)
