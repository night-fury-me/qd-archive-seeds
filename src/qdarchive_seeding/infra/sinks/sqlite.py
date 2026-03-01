from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.infra.sinks.base import BaseSink

SCHEMA = """
CREATE TABLE IF NOT EXISTS datasets (
  id TEXT PRIMARY KEY,
  source_name TEXT NOT NULL,
  source_dataset_id TEXT,
  source_url TEXT NOT NULL,
  title TEXT,
  description TEXT,
  doi TEXT,
  license TEXT,
  year INTEGER,
  owner_name TEXT,
  owner_email TEXT,
  created_at TEXT
);

CREATE TABLE IF NOT EXISTS assets (
  id TEXT PRIMARY KEY,
  dataset_id TEXT NOT NULL,
  asset_url TEXT NOT NULL,
  asset_type TEXT,
  local_dir TEXT,
  local_filename TEXT,
  downloaded_at TEXT,
  checksum_sha256 TEXT,
  size_bytes INTEGER,
  download_status TEXT,
  error_message TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dataset_unique ON datasets(source_name, source_dataset_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_asset_unique ON assets(asset_url);
"""


@dataclass(slots=True)
class SQLiteSink(BaseSink):
    path: Path

    def __post_init__(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as conn:
            conn.executescript(SCHEMA)

    def upsert_dataset(self, record: DatasetRecord) -> str:
        dataset_id = record.source_dataset_id or record.source_url
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                """
                INSERT INTO datasets (
                  id, source_name, source_dataset_id, source_url, title, description, doi, license, year,
                  owner_name, owner_email, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(source_name, source_dataset_id)
                DO UPDATE SET
                  source_url=excluded.source_url,
                  title=excluded.title,
                  description=excluded.description,
                  doi=excluded.doi,
                  license=excluded.license,
                  year=excluded.year,
                  owner_name=excluded.owner_name,
                  owner_email=excluded.owner_email
                """,
                (
                    dataset_id,
                    record.source_name,
                    record.source_dataset_id,
                    record.source_url,
                    record.title,
                    record.description,
                    record.doi,
                    record.license,
                    record.year,
                    record.owner_name,
                    record.owner_email,
                ),
            )
        return dataset_id

    def upsert_asset(self, dataset_id: str, asset: AssetRecord) -> None:
        asset_id = asset.asset_url
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                """
                INSERT INTO assets (
                  id, dataset_id, asset_url, asset_type, local_dir, local_filename,
                  downloaded_at, checksum_sha256, size_bytes, download_status, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(asset_url)
                DO UPDATE SET
                  asset_type=excluded.asset_type,
                  local_dir=excluded.local_dir,
                  local_filename=excluded.local_filename,
                  downloaded_at=excluded.downloaded_at,
                  checksum_sha256=excluded.checksum_sha256,
                  size_bytes=excluded.size_bytes,
                  download_status=excluded.download_status,
                  error_message=excluded.error_message
                """,
                (
                    asset_id,
                    dataset_id,
                    asset.asset_url,
                    asset.asset_type,
                    asset.local_dir,
                    asset.local_filename,
                    asset.downloaded_at.isoformat() if asset.downloaded_at else None,
                    asset.checksum_sha256,
                    asset.size_bytes,
                    asset.download_status,
                    asset.error_message,
                ),
            )
