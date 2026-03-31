from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.infra.sinks.base import BaseSink

SCHEMA = """
CREATE TABLE IF NOT EXISTS projects (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  query_string TEXT,
  repository_id INTEGER,
  repository_url TEXT,
  project_url TEXT NOT NULL,
  version TEXT,
  title TEXT,
  description TEXT,
  language TEXT,
  doi TEXT,
  upload_date TEXT,
  download_date TEXT,
  download_repository_folder TEXT,
  download_project_folder TEXT,
  download_version_folder TEXT,
  download_method TEXT CHECK(download_method IN ('SCRAPING', 'API-CALL')),
  is_harvested INTEGER NOT NULL DEFAULT 0,
  harvested_from TEXT
);

CREATE TABLE IF NOT EXISTS files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id INTEGER NOT NULL,
  file_name TEXT,
  file_type TEXT,
  asset_url TEXT,
  size_bytes INTEGER,
  status TEXT NOT NULL DEFAULT 'UNKNOWN'
    CHECK(status IN ('UNKNOWN', 'SUCCESS', 'FAILED', 'SKIPPED', 'RESUMABLE')),
  error_message TEXT,
  FOREIGN KEY (project_id) REFERENCES projects(id)
);

CREATE TABLE IF NOT EXISTS keywords (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id INTEGER NOT NULL,
  keyword TEXT NOT NULL,
  FOREIGN KEY (project_id) REFERENCES projects(id)
);

CREATE TABLE IF NOT EXISTS person_role (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT 'UNKNOWN'
    CHECK(role IN ('CREATOR', 'CONTRIBUTOR', 'UNKNOWN')),
  FOREIGN KEY (project_id) REFERENCES projects(id)
);

CREATE TABLE IF NOT EXISTS licenses (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id INTEGER NOT NULL,
  license TEXT NOT NULL DEFAULT 'UNKNOWN',
  FOREIGN KEY (project_id) REFERENCES projects(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_file_unique
  ON files(project_id, file_name);
"""

_MIGRATION = """
-- Drop legacy tables from older schema
DROP TABLE IF EXISTS assets;
DROP TABLE IF EXISTS datasets;
"""

_MIGRATION_ADD_FILE_COLUMNS = [
    ("asset_url", "ALTER TABLE files ADD COLUMN asset_url TEXT"),
    ("size_bytes", "ALTER TABLE files ADD COLUMN size_bytes INTEGER"),
    ("error_message", "ALTER TABLE files ADD COLUMN error_message TEXT"),
]

_MIGRATION_ADD_PROJECT_COLUMNS = [
    ("is_harvested", "ALTER TABLE projects ADD COLUMN is_harvested INTEGER NOT NULL DEFAULT 0"),
    ("harvested_from", "ALTER TABLE projects ADD COLUMN harvested_from TEXT"),
]


@dataclass(slots=True)
class SQLiteSink(BaseSink):
    path: Path
    _conn: sqlite3.Connection = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path)
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA busy_timeout = 30000")  # wait up to 30s for locks
        self._conn.executescript(_MIGRATION)
        self._conn.executescript(SCHEMA)
        self._run_column_migrations()

    def _run_column_migrations(self) -> None:
        """Add columns to existing tables if they don't exist yet."""
        existing_files = {
            row[1] for row in self._conn.execute("PRAGMA table_info(files)").fetchall()
        }
        for col_name, ddl in _MIGRATION_ADD_FILE_COLUMNS:
            if col_name not in existing_files:
                self._conn.execute(ddl)
        existing_projects = {
            row[1] for row in self._conn.execute("PRAGMA table_info(projects)").fetchall()
        }
        for col_name, ddl in _MIGRATION_ADD_PROJECT_COLUMNS:
            if col_name not in existing_projects:
                self._conn.execute(ddl)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def _find_project_id(self, record: DatasetRecord) -> int | None:
        """Find existing project by (repository_id, download_project_folder, version).

        Falls back to matching on project_url when download_project_folder is not set.
        """
        if record.download_project_folder is not None:
            if record.version is None:
                row = self._conn.execute(
                    """SELECT id FROM projects
                       WHERE repository_id IS ? AND download_project_folder = ?
                       AND version IS NULL""",
                    (record.repository_id, record.download_project_folder),
                ).fetchone()
            else:
                row = self._conn.execute(
                    """SELECT id FROM projects
                       WHERE repository_id IS ? AND download_project_folder = ?
                       AND version = ?""",
                    (record.repository_id, record.download_project_folder, record.version),
                ).fetchone()
        else:
            # Fallback: match on project_url (source_url) for extractors that
            # don't populate the new folder fields (e.g. static_list).
            row = self._conn.execute(
                "SELECT id FROM projects WHERE project_url = ?",
                (record.source_url,),
            ).fetchone()
        return row[0] if row else None

    def upsert_dataset(self, record: DatasetRecord) -> str:
        existing_id = self._find_project_id(record)

        values = (
            record.query_string,
            record.repository_id,
            record.repository_url,
            record.source_url,
            record.version,
            record.title,
            record.description,
            record.language,
            record.doi,
            record.upload_date,
            record.download_date,
            record.download_repository_folder,
            record.download_project_folder,
            record.download_version_folder,
            record.download_method,
            int(record.is_harvested),
            record.harvested_from,
        )

        if existing_id is not None:
            self._conn.execute(
                """UPDATE projects SET
                  query_string=?, repository_id=?, repository_url=?, project_url=?,
                  version=?, title=?, description=?, language=?, doi=?,
                  upload_date=?, download_date=?,
                  download_repository_folder=?, download_project_folder=?,
                  download_version_folder=?, download_method=?,
                  is_harvested=?, harvested_from=?
                WHERE id=?""",
                (*values, existing_id),
            )
            project_id = existing_id
        else:
            cursor = self._conn.execute(
                """INSERT INTO projects (
                  query_string, repository_id, repository_url, project_url,
                  version, title, description, language, doi,
                  upload_date, download_date,
                  download_repository_folder, download_project_folder,
                  download_version_folder, download_method,
                  is_harvested, harvested_from
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                values,
            )
            project_id = cursor.lastrowid or 0

        pid = str(project_id)

        # Replace keywords
        self._conn.execute("DELETE FROM keywords WHERE project_id = ?", (project_id,))
        for kw in record.keywords:
            self._conn.execute(
                "INSERT INTO keywords (project_id, keyword) VALUES (?, ?)",
                (project_id, kw),
            )

        # Replace persons
        self._conn.execute("DELETE FROM person_role WHERE project_id = ?", (project_id,))
        for person in record.persons:
            self._conn.execute(
                "INSERT INTO person_role (project_id, name, role) VALUES (?, ?, ?)",
                (project_id, person.name, person.role),
            )

        # Replace license
        self._conn.execute("DELETE FROM licenses WHERE project_id = ?", (project_id,))
        if record.license:
            self._conn.execute(
                "INSERT INTO licenses (project_id, license) VALUES (?, ?)",
                (project_id, record.license),
            )

        self._conn.commit()
        return pid

    def upsert_asset(self, dataset_id: str, asset: AssetRecord) -> None:
        project_id = int(dataset_id)
        file_name = asset.local_filename or asset.asset_url.rsplit("/", 1)[-1]
        file_type = asset.file_type or (file_name.rsplit(".", 1)[-1] if "." in file_name else None)
        status = asset.download_status or "UNKNOWN"
        error_message = asset.error_message if status == "FAILED" else None
        self._conn.execute(
            """
            INSERT INTO files
              (project_id, file_name, file_type, asset_url, size_bytes, status, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(project_id, file_name)
            DO UPDATE SET
              file_type=excluded.file_type,
              asset_url=COALESCE(excluded.asset_url, asset_url),
              size_bytes=COALESCE(excluded.size_bytes, size_bytes),
              status=CASE
                WHEN excluded.status = 'UNKNOWN' THEN files.status
                ELSE excluded.status
              END,
              error_message=CASE
                WHEN excluded.status = 'UNKNOWN' THEN files.error_message
                ELSE excluded.error_message
              END
            """,
            (
                project_id,
                file_name,
                file_type,
                asset.asset_url,
                asset.size_bytes,
                status,
                error_message,
            ),
        )
        self._conn.commit()

    def update_file_status(self, dataset_id: str, file_name: str, status: str) -> None:
        """Update the download status of a specific file."""
        project_id = int(dataset_id)
        self._conn.execute(
            "UPDATE files SET status = ? WHERE project_id = ? AND file_name = ?",
            (status, project_id, file_name),
        )
        self._conn.commit()

    def get_file_statuses(self, dataset_id: str) -> dict[str, str]:
        """Return a mapping of lookup_key → status for a project.

        Keys include both file_name and asset_url so the runner can match
        regardless of whether the asset record carries a local filename or
        only the original download URL.
        """
        project_id = int(dataset_id)
        rows = self._conn.execute(
            "SELECT file_name, asset_url, status FROM files WHERE project_id = ?",
            (project_id,),
        ).fetchall()
        result: dict[str, str] = {}
        for row in rows:
            fname, url, status = row[0], row[1], row[2]
            # Prefer non-UNKNOWN statuses: don't overwrite a meaningful status
            for key in (fname, url):
                if key and (key not in result or result[key] == "UNKNOWN"):
                    result[key] = status
        return result

    def get_existing_dataset_ids(self, repository_id: int) -> set[str]:
        """Return all download_project_folder values for a repository.

        Used to pre-populate seen_ids on resume so already-stored records
        are skipped without re-processing.
        """
        rows = self._conn.execute(
            "SELECT download_project_folder FROM projects WHERE repository_id = ?",
            (repository_id,),
        ).fetchall()
        return {row[0] for row in rows if row[0]}

    def get_pending_download_datasets(
        self, repository_id: int | None = None
    ) -> list[tuple[str, DatasetRecord, list[AssetRecord]]]:
        """Load datasets that have files not yet successfully downloaded.

        Returns a list of (dataset_id, DatasetRecord, [AssetRecord, ...]) tuples
        for projects that have at least one file with status != 'SUCCESS'.
        Only includes files that have asset_url stored (needed for download).
        """
        query = """
            SELECT DISTINCT p.id, p.query_string, p.repository_id, p.repository_url,
                   p.project_url, p.version, p.title, p.description, p.language,
                   p.doi, p.upload_date, p.download_date,
                   p.download_repository_folder, p.download_project_folder,
                   p.download_version_folder, p.download_method,
                   p.is_harvested, p.harvested_from
            FROM projects p
            JOIN files f ON f.project_id = p.id
            WHERE f.status != 'SUCCESS' AND f.asset_url IS NOT NULL
        """
        params: list[int] = []
        if repository_id is not None:
            query += " AND p.repository_id = ?"
            params.append(repository_id)

        rows = self._conn.execute(query, params).fetchall()
        results: list[tuple[str, DatasetRecord, list[AssetRecord]]] = []

        for row in rows:
            pid = row[0]
            record = DatasetRecord(
                source_name=row[12] or "",  # download_repository_folder
                source_dataset_id=str(row[13]),  # download_project_folder
                source_url=row[4],
                query_string=row[1],
                repository_id=row[2],
                repository_url=row[3],
                version=row[5],
                title=row[6],
                description=row[7],
                language=row[8],
                doi=row[9],
                upload_date=row[10],
                download_date=row[11],
                download_repository_folder=row[12],
                download_project_folder=row[13],
                download_version_folder=row[14],
                download_method=row[15],
                is_harvested=bool(row[16]),
                harvested_from=row[17],
            )

            file_rows = self._conn.execute(
                "SELECT file_name, file_type, asset_url, size_bytes, status "
                "FROM files WHERE project_id = ?",
                (pid,),
            ).fetchall()
            assets = [
                AssetRecord(
                    asset_url=fr[2] or "",
                    local_filename=fr[0],
                    file_type=fr[1],
                    size_bytes=fr[3],
                    download_status=fr[4],
                )
                for fr in file_rows
                if fr[2]  # only include files with asset_url
            ]
            record.assets = assets
            results.append((str(pid), record, assets))

        return results
