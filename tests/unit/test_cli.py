from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from qdarchive_seeding.cli.main import app

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
