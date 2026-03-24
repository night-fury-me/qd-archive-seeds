from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class QueryCheckpoint:
    query_label: str
    completed: bool = False
    last_page: int = 0
    records_yielded: int = 0


@dataclass(slots=True)
class CheckpointManager:
    """Persists query-level progress so Phase 1 can resume after interruption."""

    _path: Path
    _pipeline_id: str
    _queries: dict[str, QueryCheckpoint] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._load()

    @property
    def file_path(self) -> Path:
        return self._path / f"{self._pipeline_id}_checkpoint.json"

    def _load(self) -> None:
        fp = self.file_path
        if not fp.exists():
            return
        try:
            data = json.loads(fp.read_text())
            for label, qdata in data.get("queries", {}).items():
                self._queries[label] = QueryCheckpoint(
                    query_label=label,
                    completed=qdata.get("completed", False),
                    last_page=qdata.get("last_page", 0),
                    records_yielded=qdata.get("records_yielded", 0),
                )
            logger.info(
                "Loaded checkpoint: %d queries (%d completed)",
                len(self._queries),
                sum(1 for q in self._queries.values() if q.completed),
            )
        except (json.JSONDecodeError, KeyError) as exc:
            logger.warning("Corrupt checkpoint file, starting fresh: %s", exc)
            self._queries.clear()

    def _save(self) -> None:
        data = {
            "pipeline_id": self._pipeline_id,
            "queries": {
                label: {
                    "completed": q.completed,
                    "last_page": q.last_page,
                    "records_yielded": q.records_yielded,
                }
                for label, q in self._queries.items()
            },
        }
        self.file_path.write_text(json.dumps(data, indent=2))

    def mark_page(self, query_label: str, page: int, records: int) -> None:
        """Update last successful page for a query. Auto-saves."""
        if query_label not in self._queries:
            self._queries[query_label] = QueryCheckpoint(query_label=query_label)
        q = self._queries[query_label]
        q.last_page = page
        q.records_yielded += records
        self._save()

    def mark_query_complete(self, query_label: str) -> None:
        """Mark a query as fully completed. Auto-saves."""
        if query_label not in self._queries:
            self._queries[query_label] = QueryCheckpoint(query_label=query_label)
        self._queries[query_label].completed = True
        self._save()

    def is_query_complete(self, query_label: str) -> bool:
        q = self._queries.get(query_label)
        return q is not None and q.completed

    def get_start_page(self, query_label: str) -> int:
        """Return the page to resume from (last_page), or 0 if fresh."""
        q = self._queries.get(query_label)
        if q is None:
            return 0
        return q.last_page

    def clear(self) -> None:
        """Delete checkpoint file (called on successful run completion)."""
        fp = self.file_path
        if fp.exists():
            fp.unlink()
            logger.info("Checkpoint cleared: %s", fp)
        self._queries.clear()
