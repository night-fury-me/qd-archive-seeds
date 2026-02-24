from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(slots=True)
class ContextFormatter(logging.Formatter):
    run_id: str | None = None
    pipeline_id: str | None = None

    def format(self, record: logging.LogRecord) -> str:
        record.run_id = getattr(record, "run_id", None) or self.run_id
        record.pipeline_id = getattr(record, "pipeline_id", None) or self.pipeline_id
        record.asctime = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat()
        return super().format(record)
