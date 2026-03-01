from __future__ import annotations

import threading
from pathlib import Path
from typing import Any
from collections.abc import Iterable

from qdarchive_seeding.app.container import build_container
from qdarchive_seeding.app.progress import Completed, CountersUpdated, ProgressEvent, StageChanged
from qdarchive_seeding.app.runner import ETLRunner
from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord, RunInfo
from qdarchive_seeding.app.config_models import PipelineConfig


def _build_test_container(tmp_path: Path, config: PipelineConfig) -> Any:
    return build_container(
        config,
        runs_dir=tmp_path / "runs",
    )


def test_dry_run_no_downloads(
    tmp_path: Path, minimal_config: PipelineConfig
) -> None:
    # Override sink path to use tmp
    minimal_config.sink.options["path"] = str(tmp_path / "test.sqlite")
    container = _build_test_container(tmp_path, minimal_config)

    events: list[ProgressEvent] = []
    container.progress_bus.subscribe(events.append)

    runner = ETLRunner(container)
    info = runner.run(dry_run=True)

    assert info.counts["downloaded"] == 0
    assert any(isinstance(e, Completed) for e in events)
    assert any(isinstance(e, StageChanged) and e.stage == "done" for e in events)


def test_run_with_static_records(
    tmp_path: Path, minimal_config: PipelineConfig
) -> None:
    minimal_config.sink.options["path"] = str(tmp_path / "test.sqlite")
    minimal_config.extractor.options = {
        "records": [
            {
                "id": "rec-1",
                "source_url": "https://example.com/1",
                "title": "Record 1",
                "assets": [],
            }
        ]
    }
    container = _build_test_container(tmp_path, minimal_config)

    events: list[ProgressEvent] = []
    container.progress_bus.subscribe(events.append)

    runner = ETLRunner(container)
    info = runner.run(dry_run=False)

    assert info.counts["extracted"] == 1
    counter_events = [e for e in events if isinstance(e, CountersUpdated)]
    assert len(counter_events) > 0


def test_completed_event_is_last(
    tmp_path: Path, minimal_config: PipelineConfig
) -> None:
    minimal_config.sink.options["path"] = str(tmp_path / "test.sqlite")
    container = _build_test_container(tmp_path, minimal_config)

    events: list[ProgressEvent] = []
    container.progress_bus.subscribe(events.append)

    runner = ETLRunner(container)
    runner.run(dry_run=True)

    assert isinstance(events[-1], Completed)
