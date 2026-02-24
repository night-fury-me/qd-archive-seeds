from __future__ import annotations

import logging
from dataclasses import dataclass


@dataclass(slots=True)
class ContextFilter(logging.Filter):
    run_id: str | None = None
    pipeline_id: str | None = None
    component: str | None = None

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = getattr(record, "run_id", None) or self.run_id
        record.pipeline_id = getattr(record, "pipeline_id", None) or self.pipeline_id
        record.component = getattr(record, "component", None) or self.component
        return True
