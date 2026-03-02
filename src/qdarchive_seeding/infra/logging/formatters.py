from __future__ import annotations

import logging
from datetime import UTC, datetime


class ContextFormatter(logging.Formatter):
    def __init__(
        self,
        fmt: str | None = None,
        *,
        run_id: str | None = None,
        pipeline_id: str | None = None,
    ) -> None:
        super().__init__(fmt)
        self.run_id = run_id
        self.pipeline_id = pipeline_id

    def format(self, record: logging.LogRecord) -> str:
        record.run_id = getattr(record, "run_id", None) or self.run_id
        record.pipeline_id = getattr(record, "pipeline_id", None) or self.pipeline_id
        record.asctime = datetime.fromtimestamp(record.created, tz=UTC).isoformat()
        return super().format(record)
