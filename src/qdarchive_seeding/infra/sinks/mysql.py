from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pymysql  # type: ignore[import-untyped]

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.infra.sinks.base import BaseSink

SCHEMA_DATASETS = """
CREATE TABLE IF NOT EXISTS datasets (
  id VARCHAR(255) PRIMARY KEY,
  source_name VARCHAR(255) NOT NULL,
  source_dataset_id VARCHAR(255),
  source_url TEXT NOT NULL,
  title TEXT,
  description TEXT,
  doi VARCHAR(255),
  license VARCHAR(255),
  year INT,
  owner_name VARCHAR(255),
  owner_email VARCHAR(255),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY idx_dataset_unique (source_name, source_dataset_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

SCHEMA_ASSETS = """
CREATE TABLE IF NOT EXISTS assets (
  id VARCHAR(512) PRIMARY KEY,
  dataset_id VARCHAR(255) NOT NULL,
  asset_url TEXT NOT NULL,
  asset_type VARCHAR(50),
  local_dir TEXT,
  local_filename VARCHAR(255),
  downloaded_at DATETIME,
  checksum_sha256 VARCHAR(64),
  size_bytes BIGINT,
  download_status VARCHAR(20),
  error_message TEXT,
  UNIQUE KEY idx_asset_unique (asset_url(512))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


@dataclass(slots=True)
class MySQLSink(BaseSink):
    host: str = "localhost"
    port: int = 3306
    database: str = "qdarchive"
    user: str = "root"
    password: str = field(default="", repr=False)
    _conn: pymysql.Connection[Any] | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        conn = self._ensure_connected()
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DATASETS)
            cur.execute(SCHEMA_ASSETS)
        conn.commit()

    def _connect(self) -> pymysql.Connection[Any]:
        return pymysql.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            charset="utf8mb4",
        )

    def _ensure_connected(self) -> pymysql.Connection[Any]:
        if self._conn is None or not self._conn.open:
            self._conn = self._connect()
        return self._conn

    def upsert_dataset(self, record: DatasetRecord) -> str:
        dataset_id = record.source_dataset_id or record.source_url
        conn = self._ensure_connected()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO datasets (
                  id, source_name, source_dataset_id, source_url, title, description,
                  doi, license, year, owner_name, owner_email
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                  source_url=VALUES(source_url),
                  title=VALUES(title),
                  description=VALUES(description),
                  doi=VALUES(doi),
                  license=VALUES(license),
                  year=VALUES(year),
                  owner_name=VALUES(owner_name),
                  owner_email=VALUES(owner_email)
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
        conn.commit()
        return dataset_id

    def upsert_asset(self, dataset_id: str, asset: AssetRecord) -> None:
        asset_id = asset.asset_url
        conn = self._ensure_connected()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO assets (
                  id, dataset_id, asset_url, asset_type, local_dir, local_filename,
                  downloaded_at, checksum_sha256, size_bytes, download_status, error_message
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                  asset_type=VALUES(asset_type),
                  local_dir=VALUES(local_dir),
                  local_filename=VALUES(local_filename),
                  downloaded_at=VALUES(downloaded_at),
                  checksum_sha256=VALUES(checksum_sha256),
                  size_bytes=VALUES(size_bytes),
                  download_status=VALUES(download_status),
                  error_message=VALUES(error_message)
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
        conn.commit()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
