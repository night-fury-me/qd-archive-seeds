from __future__ import annotations

import runpy
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from typer.testing import CliRunner
from rich.console import Console

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


def test_validate_config_valid(config_yaml_path: Path) -> None:
    result = runner.invoke(app, ["seed", "validate-config", "--config", str(config_yaml_path)])
    assert result.exit_code == 0
    assert "Valid" in result.output


def test_validate_config_invalid(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text("not: valid: config")
    result = runner.invoke(app, ["seed", "validate-config", "--config", str(bad)])
    assert result.exit_code == 1


def test_validate_config_missing() -> None:
    result = runner.invoke(app, ["seed", "validate-config", "--config", "/nonexistent.yaml"])
    assert result.exit_code == 1


def test_status_missing_db() -> None:
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

        def run(self, *, dry_run: bool = False) -> None:
            captured["dry_run"] = dry_run

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
            "--force",
            "--retry-failed",
        ],
    )

    assert result.exit_code == 0
    assert captured["subscribed"] is True
    assert captured["runner_init"] is True
    assert captured["dry_run"] is True
    assert captured["config"].pipeline.max_items == 5


def test_run_pipeline_config_error(monkeypatch: object) -> None:
    def fake_load_config(_path: Path) -> Any:
        raise ConfigError("bad config")

    monkeypatch.setattr(seed_module, "load_config", fake_load_config)

    result = runner.invoke(app, ["seed", "run", "--config", "missing.yaml"])

    assert result.exit_code == 1
    assert "Config error" in result.output


def test_cli_progress_display_events() -> None:
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


def test_status_command_with_db(tmp_path: Path) -> None:
    db = tmp_path / "qdarchive.sqlite"
    conn = sqlite3.connect(db)
    try:
        conn.executescript(
            """
            CREATE TABLE datasets (id TEXT);
            CREATE TABLE assets (id TEXT, download_status TEXT);
            INSERT INTO datasets (id) VALUES ('d1'), ('d2');
            INSERT INTO assets (id, download_status) VALUES
              ('a1', 'SUCCESS'),
              ('a2', 'FAILED'),
              ('a3', 'SKIPPED');
            """
        )
        conn.commit()
    finally:
        conn.close()

    result = runner.invoke(app, ["seed", "status", "--db", str(db)])

    assert result.exit_code == 0
    assert "Datasets" in result.output
    assert "Total assets" in result.output


def test_export_csv_and_excel(tmp_path: Path) -> None:
    db = tmp_path / "qdarchive.sqlite"
    conn = sqlite3.connect(db)
    try:
        conn.executescript(
            """
            CREATE TABLE datasets (id TEXT, source_name TEXT);
            CREATE TABLE assets (id TEXT, asset_url TEXT, download_status TEXT);
            INSERT INTO datasets (id, source_name) VALUES ('d1', 's');
            INSERT INTO assets (id, asset_url, download_status) VALUES ('a1', 'u', 'SUCCESS');
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
    assert (tmp_path / "export" / "datasets.csv").exists()
    assert (tmp_path / "export" / "assets.csv").exists()

    excel_out = tmp_path / "export.xlsx"
    result = runner.invoke(
        app,
        ["seed", "export", "--db", str(db), "--format", "excel", "--out", str(excel_out)],
    )
    assert result.exit_code == 0
    assert excel_out.exists()


def test_export_unknown_format(tmp_path: Path) -> None:
    db = tmp_path / "qdarchive.sqlite"
    conn = sqlite3.connect(db)
    try:
        conn.executescript(
            """
            CREATE TABLE datasets (id TEXT);
            CREATE TABLE assets (id TEXT);
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


def test_export_missing_db() -> None:
    result = runner.invoke(
        app,
        ["seed", "export", "--db", "/nonexistent.sqlite", "--format", "csv", "--out", "./x"],
    )

    assert result.exit_code == 1
    assert "Database not found" in result.output


def test_cli_main_module_executes(monkeypatch: object) -> None:
    monkeypatch.setattr("sys.argv", ["qdarchive", "--help"])  # type: ignore[attr-defined]
    import sys

    sys.modules.pop("qdarchive_seeding.cli.main", None)
    try:
        runpy.run_module("qdarchive_seeding.cli.main", run_name="__main__")
        assert False, "Expected SystemExit from CLI entrypoint"
    except SystemExit as exc:
        assert exc.code in (0, None)
