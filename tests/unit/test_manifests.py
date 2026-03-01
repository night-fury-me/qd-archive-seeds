from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from qdarchive_seeding.app.manifests import RunManifestWriter
from qdarchive_seeding.core.entities import RunInfo


def _make_run_info(run_id: str = "test-run-1") -> RunInfo:
    return RunInfo(
        run_id=run_id,  # type: ignore[arg-type]
        pipeline_id="test_pipe",
        started_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        ended_at=datetime(2024, 1, 1, 12, 5, 0, tzinfo=timezone.utc),
        config_hash="abc123",
        counts={"extracted": 10, "downloaded": 8, "failed": 2},
        failures=[{"asset_url": "https://example.com/bad.pdf", "error": "timeout"}],
    )


def test_write_and_load(tmp_path: Path) -> None:
    writer = RunManifestWriter(runs_dir=tmp_path / "runs")
    info = _make_run_info()
    path = writer.write(info)
    assert path.exists()
    loaded = writer.load("test-run-1")
    assert loaded["run_id"] == "test-run-1"
    assert loaded["pipeline_id"] == "test_pipe"
    assert loaded["counts"]["extracted"] == 10


def test_list_runs_sorted(tmp_path: Path) -> None:
    writer = RunManifestWriter(runs_dir=tmp_path / "runs")
    writer.write(_make_run_info("run-a"))
    info2 = _make_run_info("run-b")
    info2.started_at = datetime(2024, 6, 1, tzinfo=timezone.utc)
    writer.write(info2)
    runs = writer.list_runs()
    assert len(runs) == 2
    assert runs[0]["run_id"] == "run-b"
