from __future__ import annotations

import logging
import queue
from pathlib import Path

from qdarchive_seeding.app.config_models import LoggingSettings
from qdarchive_seeding.infra.logging.handlers import UILogQueueHandler
from qdarchive_seeding.infra.logging.logger import configure_logger


def test_configure_logger_with_queue() -> None:
    settings = LoggingSettings(level="DEBUG", console={"enabled": False}, file={"enabled": False})
    bundle = configure_logger(
        "test_queue_logger", settings, run_id="r1", pipeline_id="p1", enable_queue=True
    )
    assert bundle.log_queue is not None
    bundle.logger.info("test message")
    assert not bundle.log_queue.empty()


def test_configure_logger_without_queue() -> None:
    settings = LoggingSettings(level="DEBUG", console={"enabled": False}, file={"enabled": False})
    bundle = configure_logger("test_no_queue_logger", settings, enable_queue=False)
    assert bundle.log_queue is None


def test_context_filter_injects_fields() -> None:
    settings = LoggingSettings(level="DEBUG", console={"enabled": False}, file={"enabled": False})
    bundle = configure_logger(
        "test_context_logger",
        settings,
        run_id="run-123",
        pipeline_id="pipe-456",
        component="extractor",
        enable_queue=True,
    )
    bundle.logger.info("context test")
    assert bundle.log_queue is not None
    msg = bundle.log_queue.get_nowait()
    assert "run-123" in msg
    assert "pipe-456" in msg


def test_file_logger_creates_parent_directory(tmp_path: Path) -> None:
    log_path = tmp_path / "nested" / "logs" / "qdarchive.log"
    settings = LoggingSettings(
        level="INFO",
        console={"enabled": False},
        file={"enabled": True, "path": str(log_path)},
    )

    bundle = configure_logger("test_file_logger", settings)
    bundle.logger.info("file logging works")

    assert log_path.parent.exists()
    assert log_path.exists()


def test_file_logger_missing_path_raises() -> None:
    settings = LoggingSettings(
        level="INFO",
        console={"enabled": False},
        file={"enabled": True, "path": None},
    )

    try:
        configure_logger("test_file_logger", settings)
    except ValueError as exc:
        assert "no path" in str(exc)
    else:
        raise AssertionError("Expected ValueError when file path is missing")


def test_queue_handler_handles_formatter_errors() -> None:
    log_queue: queue.Queue[str] = queue.Queue()
    handler = UILogQueueHandler(log_queue)

    class BadFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            raise RuntimeError("formatting failed")

    handler.setFormatter(BadFormatter())
    called: list[bool] = []

    def fake_handle_error(_record: logging.LogRecord) -> None:
        called.append(True)

    handler.handleError = fake_handle_error  # type: ignore[assignment]
    record = logging.LogRecord("test", logging.INFO, __file__, 1, "msg", (), None)
    handler.emit(record)

    assert called == [True]
