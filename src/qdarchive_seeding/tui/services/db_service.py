from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class StatusCounts:
    datasets: int = 0
    total_assets: int = 0
    success: int = 0
    failed: int = 0
    skipped: int = 0
    resumable: int = 0


class DBService:
    def __init__(self, db_path: Path = Path("./metadata/qdarchive.sqlite")) -> None:
        self._db_path = db_path

    def get_status(self) -> StatusCounts | None:
        if not self._db_path.exists():
            return None
        conn = sqlite3.connect(self._db_path)
        try:
            counts = StatusCounts()
            row = conn.execute("SELECT COUNT(*) FROM datasets").fetchone()
            counts.datasets = row[0] if row else 0
            row = conn.execute("SELECT COUNT(*) FROM assets").fetchone()
            counts.total_assets = row[0] if row else 0
            for status_name in ("SUCCESS", "FAILED", "SKIPPED", "RESUMABLE"):
                row = conn.execute(
                    "SELECT COUNT(*) FROM assets WHERE download_status = ?", (status_name,)
                ).fetchone()
                setattr(counts, status_name.lower(), row[0] if row else 0)
            return counts
        finally:
            conn.close()

    def get_recent_datasets(self, limit: int = 20) -> list[dict[str, Any]]:
        if not self._db_path.exists():
            return []
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                "SELECT * FROM datasets ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()
