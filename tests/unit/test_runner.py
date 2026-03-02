from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

from qdarchive_seeding.app.config_models import LoggingSettings, PipelineConfig
from qdarchive_seeding.app.container import Container, build_container
from qdarchive_seeding.app.progress import (
    Completed,
    CountersUpdated,
    ErrorEvent,
    ProgressBus,
    ProgressEvent,
    StageChanged,
)
from qdarchive_seeding.app.runner import ETLRunner
from qdarchive_seeding.core.constants import DOWNLOAD_STATUS_SKIPPED
from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.infra.logging.logger import configure_logger
from qdarchive_seeding.infra.storage.filesystem import FileSystem
from qdarchive_seeding.infra.storage.paths import PathStrategy
from qdarchive_seeding.infra.transforms.base import TransformChain


def _build_test_container(tmp_path: Path, config: PipelineConfig) -> Any:
    return build_container(
        config,
        runs_dir=tmp_path / "runs",
    )


def test_dry_run_no_downloads(tmp_path: Path, minimal_config: PipelineConfig) -> None:
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


def test_run_with_static_records(tmp_path: Path, minimal_config: PipelineConfig) -> None:
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


def test_completed_event_is_last(tmp_path: Path, minimal_config: PipelineConfig) -> None:
    minimal_config.sink.options["path"] = str(tmp_path / "test.sqlite")
    container = _build_test_container(tmp_path, minimal_config)

    events: list[ProgressEvent] = []
    container.progress_bus.subscribe(events.append)

    runner = ETLRunner(container)
    runner.run(dry_run=True)

    assert isinstance(events[-1], Completed)


class _ListExtractor:
    def __init__(self, records: Iterable[DatasetRecord]) -> None:
        self._records = list(records)

    def extract(self, _ctx: Any) -> Iterable[DatasetRecord]:
        return list(self._records)


class _ExplodingExtractor:
    def extract(self, _ctx: Any) -> Iterable[DatasetRecord]:
        raise RuntimeError("boom")


class _CancelExtractor:
    def extract(self, ctx: Any) -> Iterable[DatasetRecord]:
        ctx.cancelled = True
        return [
            DatasetRecord(source_name="s", source_dataset_id="1", source_url="u"),
        ]


class _Downloader:
    def __init__(self, *, should_fail: bool = False) -> None:
        self.should_fail = should_fail
        self.on_progress = None

    def download(self, asset: AssetRecord, _target_dir: Path):
        if self.on_progress is not None:
            self.on_progress(1, 1)
        if self.should_fail:
            raise RuntimeError("download failed")
        asset.download_status = "SUCCESS"
        return type("Result", (), {"asset": asset, "bytes_downloaded": 1, "checksum": ""})


class _Sink:
    def __init__(self, *, should_fail: bool = False) -> None:
        self.should_fail = should_fail
        self.closed = False

    def upsert_dataset(self, _record: DatasetRecord) -> str:
        if self.should_fail:
            raise RuntimeError("sink failed")
        return "ds-1"

    def upsert_asset(self, _dataset_id: str, _asset: AssetRecord) -> None:
        return None

    def close(self) -> None:
        self.closed = True


class _Policy:
    def __init__(self, *, skip: bool) -> None:
        self._skip = skip

    def should_skip_asset(self, _asset: AssetRecord) -> bool:
        return self._skip

    def should_retry(self, _error: Exception, _attempt: int) -> bool:
        return False


class _Transform:
    def __init__(self, name: str, drop: bool = False) -> None:
        self.name = name
        self.drop = drop

    def apply(self, record: DatasetRecord) -> DatasetRecord | None:
        return None if self.drop else record


def _make_container(
    tmp_path: Path,
    config: PipelineConfig,
    *,
    extractor: Any,
    pre_transforms: list[Any],
    post_transforms: list[Any],
    downloader: Any,
    sink: Any,
    policy: Any,
) -> Container:
    config.storage.downloads_root = str(tmp_path / "downloads")
    logger_bundle = configure_logger(
        "test_runner",
        LoggingSettings(level="DEBUG", console={"enabled": False}, file={"enabled": False}),
    )
    return Container(
        config=config,
        run_id="run-1",
        logger_bundle=logger_bundle,
        auth=None,  # type: ignore[arg-type]
        http_client=None,  # type: ignore[arg-type]
        rate_limiter=type("Limiter", (), {"wait": lambda self: None})(),
        extractor=extractor,
        pre_transform_chain=TransformChain(transforms=pre_transforms),
        post_transform_chain=TransformChain(transforms=post_transforms),
        downloader=downloader,
        sink=sink,
        policy=policy,
        progress_bus=ProgressBus(),
        manifests=type("Manifests", (), {"write": lambda self, _info: None})(),
        path_strategy=PathStrategy(layout_template="{source_name}/{dataset_slug}/"),
        filesystem=FileSystem(root=tmp_path),
        config_hash="hash",
    )


def test_runner_download_and_sink_errors(tmp_path: Path, minimal_config: PipelineConfig) -> None:
    record = DatasetRecord(
        source_name="s",
        source_dataset_id="1",
        source_url="u",
        assets=[AssetRecord(asset_url="https://example.com/a")],
        raw={"dataset_slug": "ds"},
    )
    container = _make_container(
        tmp_path,
        minimal_config,
        extractor=_ListExtractor([record]),
        pre_transforms=[_Transform("pre")],
        post_transforms=[_Transform("post")],
        downloader=_Downloader(should_fail=True),
        sink=_Sink(should_fail=True),
        policy=_Policy(skip=False),
    )
    events: list[ProgressEvent] = []
    container.progress_bus.subscribe(events.append)

    runner = ETLRunner(container)
    info = runner.run(dry_run=False)

    assert info.counts["failed"] == 1
    assert any(isinstance(e, ErrorEvent) and e.component == "downloader" for e in events)
    assert any(isinstance(e, ErrorEvent) and e.component == "sink" for e in events)


def test_runner_respects_policy_skip_and_max_items(
    tmp_path: Path, minimal_config: PipelineConfig
) -> None:
    minimal_config.pipeline.max_items = 1
    record1 = DatasetRecord(
        source_name="s",
        source_dataset_id="1",
        source_url="u",
        assets=[AssetRecord(asset_url="https://example.com/a")],
        raw={"dataset_slug": "ds"},
    )
    record2 = DatasetRecord(
        source_name="s",
        source_dataset_id="2",
        source_url="u2",
        assets=[AssetRecord(asset_url="https://example.com/b")],
        raw={"dataset_slug": "ds2"},
    )

    container = _make_container(
        tmp_path,
        minimal_config,
        extractor=_ListExtractor([record1, record2]),
        pre_transforms=[_Transform("pre")],
        post_transforms=[_Transform("post")],
        downloader=_Downloader(),
        sink=_Sink(),
        policy=_Policy(skip=True),
    )

    runner = ETLRunner(container)
    info = runner.run(dry_run=False)

    assert info.counts["skipped"] == 1
    assert record1.assets[0].download_status == DOWNLOAD_STATUS_SKIPPED
    assert info.counts["transformed"] == 1


def test_runner_extractor_error_publishes_event(
    tmp_path: Path, minimal_config: PipelineConfig
) -> None:
    container = _make_container(
        tmp_path,
        minimal_config,
        extractor=_ExplodingExtractor(),
        pre_transforms=[],
        post_transforms=[],
        downloader=_Downloader(),
        sink=_Sink(),
        policy=_Policy(skip=False),
    )
    events: list[ProgressEvent] = []
    container.progress_bus.subscribe(events.append)

    runner = ETLRunner(container)
    info = runner.run(dry_run=False)

    assert info.counts["extracted"] == 0
    assert any(isinstance(e, ErrorEvent) and e.component == "extractor" for e in events)


def test_runner_pre_and_post_transform_filter(
    tmp_path: Path, minimal_config: PipelineConfig
) -> None:
    record = DatasetRecord(
        source_name="s",
        source_dataset_id="1",
        source_url="u",
        assets=[AssetRecord(asset_url="https://example.com/a")],
    )

    container = _make_container(
        tmp_path,
        minimal_config,
        extractor=_ListExtractor([record]),
        pre_transforms=[_Transform("pre", drop=True)],
        post_transforms=[_Transform("post", drop=True)],
        downloader=_Downloader(),
        sink=_Sink(),
        policy=_Policy(skip=False),
    )

    runner = ETLRunner(container)
    info = runner.run(dry_run=False)

    assert info.counts["transformed"] == 0


def test_runner_post_transform_drop(tmp_path: Path, minimal_config: PipelineConfig) -> None:
    record = DatasetRecord(
        source_name="s",
        source_dataset_id="1",
        source_url="u",
        assets=[AssetRecord(asset_url="https://example.com/a")],
    )

    container = _make_container(
        tmp_path,
        minimal_config,
        extractor=_ListExtractor([record]),
        pre_transforms=[_Transform("pre")],
        post_transforms=[_Transform("post", drop=True)],
        downloader=_Downloader(),
        sink=_Sink(),
        policy=_Policy(skip=False),
    )

    runner = ETLRunner(container)
    info = runner.run(dry_run=False)

    assert info.counts["loaded"] == 0


def test_runner_cancelled_breaks_loop(tmp_path: Path, minimal_config: PipelineConfig) -> None:
    container = _make_container(
        tmp_path,
        minimal_config,
        extractor=_CancelExtractor(),
        pre_transforms=[],
        post_transforms=[],
        downloader=_Downloader(),
        sink=_Sink(),
        policy=_Policy(skip=False),
    )

    runner = ETLRunner(container)
    info = runner.run(dry_run=False)

    assert info.counts["extracted"] == 0


def test_runner_filters_every_50_items(tmp_path: Path, minimal_config: PipelineConfig) -> None:
    records = [
        DatasetRecord(source_name="s", source_dataset_id=str(i), source_url="u") for i in range(50)
    ]
    container = _make_container(
        tmp_path,
        minimal_config,
        extractor=_ListExtractor(records),
        pre_transforms=[_Transform("pre", drop=True)],
        post_transforms=[],
        downloader=_Downloader(),
        sink=_Sink(),
        policy=_Policy(skip=False),
    )
    events: list[ProgressEvent] = []
    container.progress_bus.subscribe(events.append)

    runner = ETLRunner(container)
    runner.run(dry_run=False)

    assert any(isinstance(e, CountersUpdated) and e.extracted == 50 for e in events)


def test_runner_dry_run_skips_assets(tmp_path: Path, minimal_config: PipelineConfig) -> None:
    record = DatasetRecord(
        source_name="s",
        source_dataset_id="1",
        source_url="u",
        assets=[AssetRecord(asset_url="https://example.com/a")],
    )
    container = _make_container(
        tmp_path,
        minimal_config,
        extractor=_ListExtractor([record]),
        pre_transforms=[_Transform("pre")],
        post_transforms=[_Transform("post")],
        downloader=_Downloader(),
        sink=_Sink(),
        policy=_Policy(skip=False),
    )

    runner = ETLRunner(container)
    info = runner.run(dry_run=True)

    assert info.counts["skipped"] == 1
    assert record.assets[0].download_status == DOWNLOAD_STATUS_SKIPPED


def test_runner_cancelled_during_assets_breaks(
    tmp_path: Path, minimal_config: PipelineConfig
) -> None:
    class CancelAssets(list[AssetRecord]):
        def __init__(self, ctx: Any) -> None:
            super().__init__(
                [
                    AssetRecord(asset_url="https://example.com/a"),
                    AssetRecord(asset_url="https://example.com/b"),
                ]
            )
            self._ctx = ctx
            self._yielded = False

        def __iter__(self):
            for asset in super().__iter__():
                if self._yielded:
                    self._ctx.cancelled = True
                self._yielded = True
                yield asset

    class CancelExtractor:
        def extract(self, ctx: Any) -> Iterable[DatasetRecord]:
            record = DatasetRecord(
                source_name="s",
                source_dataset_id="1",
                source_url="u",
            )
            record.assets = CancelAssets(ctx)
            record.raw = {"dataset_slug": "ds"}
            return [record]

    container = _make_container(
        tmp_path,
        minimal_config,
        extractor=CancelExtractor(),
        pre_transforms=[_Transform("pre")],
        post_transforms=[_Transform("post")],
        downloader=_Downloader(),
        sink=_Sink(),
        policy=_Policy(skip=False),
    )

    runner = ETLRunner(container)
    info = runner.run(dry_run=False)

    assert info.counts["downloaded"] <= 1


def test_runner_non_success_download_status(tmp_path: Path, minimal_config: PipelineConfig) -> None:
    record = DatasetRecord(
        source_name="s",
        source_dataset_id="1",
        source_url="u",
        assets=[AssetRecord(asset_url="https://example.com/a")],
    )

    class NonSuccessDownloader(_Downloader):
        def download(self, asset: AssetRecord, _target_dir: Path):  # type: ignore[override]
            asset.download_status = "FAILED"
            return type(
                "Result",
                (),
                {"asset": asset, "bytes_downloaded": 1, "checksum": ""},
            )

    container = _make_container(
        tmp_path,
        minimal_config,
        extractor=_ListExtractor([record]),
        pre_transforms=[_Transform("pre")],
        post_transforms=[_Transform("post")],
        downloader=NonSuccessDownloader(),
        sink=_Sink(),
        policy=_Policy(skip=False),
    )

    runner = ETLRunner(container)
    info = runner.run(dry_run=False)

    assert info.counts["failed"] == 1
