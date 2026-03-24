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
  download_method TEXT
);

CREATE TABLE IF NOT EXISTS files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id INTEGER NOT NULL,
  file_name TEXT,
  file_type TEXT,
  status TEXT DEFAULT 'UNKNOWN',
  FOREIGN KEY (project_id) REFERENCES projects(id)
);

CREATE TABLE IF NOT EXISTS keywords (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id INTEGER NOT NULL,
  keyword TEXT,
  FOREIGN KEY (project_id) REFERENCES projects(id)
);

CREATE TABLE IF NOT EXISTS person_role (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id INTEGER NOT NULL,
  name TEXT,
  role TEXT DEFAULT 'UNKNOWN',
  FOREIGN KEY (project_id) REFERENCES projects(id)
);

CREATE TABLE IF NOT EXISTS licenses (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id INTEGER NOT NULL,
  license TEXT DEFAULT 'UNKNOWN',
  FOREIGN KEY (project_id) REFERENCES projects(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_file_unique
  ON files(project_id, file_name);
"""


@dataclass(slots=True)
class SQLiteSink(BaseSink):
    path: Path
    _conn: sqlite3.Connection = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path)
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.executescript(SCHEMA)

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
        )

        if existing_id is not None:
            self._conn.execute(
                """UPDATE projects SET
                  query_string=?, repository_id=?, repository_url=?, project_url=?,
                  version=?, title=?, description=?, language=?, doi=?,
                  upload_date=?, download_date=?,
                  download_repository_folder=?, download_project_folder=?,
                  download_version_folder=?, download_method=?
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
                  download_version_folder, download_method
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
        file_type = asset.file_type or (
            file_name.rsplit(".", 1)[-1] if "." in file_name else None
        )
        status = asset.download_status or "UNKNOWN"
        self._conn.execute(
            """
            INSERT INTO files (project_id, file_name, file_type, status)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(project_id, file_name)
            DO UPDATE SET
              file_type=excluded.file_type,
              status=excluded.status
            """,
            (project_id, file_name, file_type, status),
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
        """Return a mapping of file_name → status for a project.

        Used by the runner to restore prior download states so that
        already-downloaded files are skipped on resume.
        """
        project_id = int(dataset_id)
        rows = self._conn.execute(
            "SELECT file_name, status FROM files WHERE project_id = ?",
            (project_id,),
        ).fetchall()
        return {row[0]: row[1] for row in rows if row[0]}

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
