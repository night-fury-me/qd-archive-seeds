from __future__ import annotations

from qdarchive_seeding.app.config_models import LoggingSettings
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
