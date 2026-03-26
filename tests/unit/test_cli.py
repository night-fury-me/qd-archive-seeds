from __future__ import annotations

import runpy
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from rich.console import Console
from typer.testing import CliRunner

from qdarchive_seeding.app.progress import (
    AssetDownloadProgress,
    AssetDownloadUpdate,
    Completed,
    CountersUpdated,
    ErrorEvent,
    StageChanged,
)
from qdarchive_seeding.cli.commands import seed as seed_module
from qdarchive_seeding.cli.main import app
from qdarchive_seeding.core.entities import RunInfo
from qdarchive_seeding.core.exceptions import ConfigError

runner = CliRunner()


@pytest.mark.asyncio



async def test_validate_config_valid(config_yaml_path: Path) -> None:
    result = runner.invoke(app, ["seed", "validate-config", "--config", str(config_yaml_path)])
    assert result.exit_code == 0
    assert "Valid" in result.output


@pytest.mark.asyncio



async def test_validate_config_invalid(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text("not: valid: config")
    result = runner.invoke(app, ["seed", "validate-config", "--config", str(bad)])
    assert result.exit_code == 1


@pytest.mark.asyncio



async def test_validate_config_missing() -> None:
    result = runner.invoke(app, ["seed", "validate-config", "--config", "/nonexistent.yaml"])
    assert result.exit_code == 1


@pytest.mark.asyncio



async def test_status_missing_db() -> None:
    result = runner.invoke(app, ["seed", "status", "--db", "/nonexistent.sqlite"])
    assert result.exit_code == 1


def test_run_pipeline_invokes_runner(monkeypatch: object, minimal_config: Any) -> None:
    captured: dict[str, Any] = {}

    def fake_load_config(_path: Path) -> Any:
        return minimal_config

    class DummyBus:
        def subscribe(self, _cb):
            captured["subscribed"] = True
            return lambda: None

    class DummyContainer:
        progress_bus = DummyBus()

    def fake_build_container(cfg: Any, **_kwargs: Any) -> DummyContainer:
        captured["config"] = cfg
        return DummyContainer()

    class DummyRunner:
        def __init__(self, _container: Any) -> None:
            captured["runner_init"] = True

        async def run(self, **kwargs: Any) -> None:
            captured["dry_run"] = kwargs.get("dry_run", False)

    monkeypatch.setattr(seed_module, "load_config", fake_load_config)
    monkeypatch.setattr(seed_module, "build_container", fake_build_container)
    monkeypatch.setattr(seed_module, "ETLRunner", DummyRunner)

    result = runner.invoke(
        app,
        [
            "seed",
            "run",
            "--config",
            "config.yaml",
            "--dry-run",
            "--max-items",
            "5",
            "--fresh-download",
            "--retry-failed",
        ],
    )

    assert result.exit_code == 0
    assert captured["subscribed"] is True
    assert captured["runner_init"] is True
    assert captured["dry_run"] is True
    assert captured["config"].pipeline.max_items == 5


@pytest.mark.asyncio



async def test_run_pipeline_config_error(monkeypatch: object) -> None:
    def fake_load_config(_path: Path) -> Any:
        raise ConfigError("bad config")

    monkeypatch.setattr(seed_module, "load_config", fake_load_config)

    result = runner.invoke(app, ["seed", "run", "--config", "missing.yaml"])

    assert result.exit_code == 1
    assert "Config error" in result.output


@pytest.mark.asyncio



async def test_cli_progress_display_events() -> None:
    console = Console(record=True)
    seed_module.console = console
    display = seed_module.CliProgressDisplay()

    display(AssetDownloadProgress(asset_url="a", bytes_downloaded=1, total_bytes=None))
    display(StageChanged(stage="extract"))
    display(StageChanged(stage="download"))
    display(CountersUpdated(extracted=1, transformed=1, total_assets=2))
    display(AssetDownloadProgress(asset_url="a", bytes_downloaded=10, total_bytes=None))
    display(AssetDownloadProgress(asset_url="a", bytes_downloaded=10, total_bytes=100))
    display(AssetDownloadUpdate(asset_url="a", status="SUCCESS", bytes_downloaded=10))
    display(ErrorEvent(component="downloader", error_type="RuntimeError", message="boom"))

    run_info = RunInfo(
        run_id="run-1",  # type: ignore[arg-type]
        pipeline_id="pipe-1",
        started_at=datetime(2024, 1, 1, tzinfo=UTC),
        ended_at=datetime(2024, 1, 1, 1, tzinfo=UTC),
        config_hash="hash",
        counts={"extracted": 1},
        failures=[{"asset_url": "x", "error": "bad"}],
    )
    display(Completed(run_info=run_info))
    display(StageChanged(stage="done"))

    output = console.export_text()
    assert "Run Summary" in output
    assert "failures" in output


@pytest.mark.asyncio



async def test_status_command_with_db(tmp_path: Path) -> None:
    db = tmp_path / "qdarchive.sqlite"
    conn = sqlite3.connect(db)
    try:
        conn.executescript(
            """
            CREATE TABLE projects (id INTEGER PRIMARY KEY, project_url TEXT NOT NULL);
            CREATE TABLE files (
                id INTEGER PRIMARY KEY, project_id INTEGER,
                file_name TEXT, status TEXT);
            INSERT INTO projects (id, project_url)
                VALUES (1, 'u1'), (2, 'u2');
            INSERT INTO files (id, project_id, file_name, status) VALUES
              (1, 1, 'a.qdpx', 'SUCCESS'),
              (2, 1, 'b.pdf', 'FAILED'),
              (3, 2, 'c.csv', 'SKIPPED');
            """
        )
        conn.commit()
    finally:
        conn.close()

    result = runner.invoke(app, ["seed", "status", "--db", str(db)])

    assert result.exit_code == 0
    assert "Projects" in result.output
    assert "Total files" in result.output


@pytest.mark.asyncio



async def test_export_csv_and_excel(tmp_path: Path) -> None:
    db = tmp_path / "qdarchive.sqlite"
    conn = sqlite3.connect(db)
    try:
        conn.executescript(
            """
            CREATE TABLE projects (id INTEGER PRIMARY KEY, project_url TEXT NOT NULL);
            CREATE TABLE files (
                id INTEGER PRIMARY KEY, project_id INTEGER,
                file_name TEXT, status TEXT);
            INSERT INTO projects (id, project_url) VALUES (1, 'u1');
            INSERT INTO files (id, project_id, file_name, status)
                VALUES (1, 1, 'a.qdpx', 'SUCCESS');
            """
        )
        conn.commit()
    finally:
        conn.close()

    out = tmp_path / "export"
    result = runner.invoke(
        app, ["seed", "export", "--db", str(db), "--format", "csv", "--out", str(out)]
    )
    assert result.exit_code == 0
    assert (tmp_path / "export" / "projects.csv").exists()
    assert (tmp_path / "export" / "files.csv").exists()

    excel_out = tmp_path / "export.xlsx"
    result = runner.invoke(
        app,
        ["seed", "export", "--db", str(db), "--format", "excel", "--out", str(excel_out)],
    )
    assert result.exit_code == 0
    assert excel_out.exists()


@pytest.mark.asyncio



async def test_export_unknown_format(tmp_path: Path) -> None:
    db = tmp_path / "qdarchive.sqlite"
    conn = sqlite3.connect(db)
    try:
        conn.executescript(
            """
            CREATE TABLE projects (id INTEGER PRIMARY KEY, project_url TEXT NOT NULL);
            CREATE TABLE files (id INTEGER PRIMARY KEY, project_id INTEGER);
            """
        )
        conn.commit()
    finally:
        conn.close()

    result = runner.invoke(
        app,
        ["seed", "export", "--db", str(db), "--format", "parquet", "--out", "./x"],
    )

    assert result.exit_code == 1
    assert "Unknown format" in result.output


@pytest.mark.asyncio



async def test_export_missing_db() -> None:
    result = runner.invoke(
        app,
        ["seed", "export", "--db", "/nonexistent.sqlite", "--format", "csv", "--out", "./x"],
    )

    assert result.exit_code == 1
    assert "Database not found" in result.output


@pytest.mark.asyncio



async def test_cli_main_module_executes(monkeypatch: object) -> None:
    monkeypatch.setattr("sys.argv", ["qdarchive", "--help"])  # type: ignore[attr-defined]
    import sys

    sys.modules.pop("qdarchive_seeding.cli.main", None)
    try:
        runpy.run_module("qdarchive_seeding.cli.main", run_name="__main__")
        raise AssertionError("Expected SystemExit from CLI entrypoint")
    except SystemExit as exc:
        assert exc.code in (0, None)
