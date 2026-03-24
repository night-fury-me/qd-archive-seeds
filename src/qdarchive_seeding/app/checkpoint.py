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
    failed_pages: list[int] = field(default_factory=list)


@dataclass(slots=True)
class CheckpointManager:
    """Persists query-level progress so Phase 1 can resume after interruption."""

    _path: Path
    _pipeline_id: str
    _queries: dict[str, QueryCheckpoint] = field(default_factory=dict)
    _date_slices: dict[str, list[tuple[str, str]]] = field(default_factory=dict)

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
                    failed_pages=qdata.get("failed_pages", []),
                )
            for label, slices in data.get("date_slices", {}).items():
                self._date_slices[label] = [(s[0], s[1]) for s in slices if len(s) == 2]
            logger.info(
                "Loaded checkpoint: %d queries (%d completed, %d with failures)",
                len(self._queries),
                sum(1 for q in self._queries.values() if q.completed),
                sum(1 for q in self._queries.values() if q.failed_pages),
            )
        except (json.JSONDecodeError, KeyError) as exc:
            logger.warning("Corrupt checkpoint file, starting fresh: %s", exc)
            self._queries.clear()
            self._date_slices.clear()

    def _save(self) -> None:
        data: dict[str, object] = {
            "pipeline_id": self._pipeline_id,
            "queries": {
                label: {
                    "completed": q.completed,
                    "last_page": q.last_page,
                    "records_yielded": q.records_yielded,
                    "failed_pages": q.failed_pages,
                }
                for label, q in self._queries.items()
            },
        }
        if self._date_slices:
            data["date_slices"] = {
                label: [list(s) for s in slices] for label, slices in self._date_slices.items()
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

    def mark_page_failed(self, query_label: str, page: int) -> None:
        """Record a page that failed during extraction. Auto-saves."""
        if query_label not in self._queries:
            self._queries[query_label] = QueryCheckpoint(query_label=query_label)
        q = self._queries[query_label]
        if page not in q.failed_pages:
            q.failed_pages.append(page)
        self._save()

    def get_failed_pages(self, query_label: str) -> list[int]:
        q = self._queries.get(query_label)
        return list(q.failed_pages) if q else []

    def clear_failed_page(self, query_label: str, page: int) -> None:
        """Remove a page from the failed list after successful retry. Auto-saves."""
        q = self._queries.get(query_label)
        if q and page in q.failed_pages:
            q.failed_pages.remove(page)
            self._save()

    def has_unresolved_failures(self) -> bool:
        return any(q.failed_pages for q in self._queries.values())

    def get_date_slices(self, query_label: str) -> list[tuple[str, str]] | None:
        """Return cached date slices for a query, or None if not cached."""
        slices = self._date_slices.get(query_label)
        return list(slices) if slices else None

    def set_date_slices(self, query_label: str, slices: list[tuple[str, str]]) -> None:
        """Cache date slices for a query. Auto-saves."""
        self._date_slices[query_label] = list(slices)
        self._save()

    def clear(self) -> None:
        """Delete checkpoint file (called on successful run completion)."""
        fp = self.file_path
        if fp.exists():
            fp.unlink()
            logger.info("Checkpoint cleared: %s", fp)
        self._queries.clear()
        self._date_slices.clear()
