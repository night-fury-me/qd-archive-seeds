from __future__ import annotations

import logging
import platform
import sys
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from qdarchive_seeding.app.config_models import PipelineConfig
from qdarchive_seeding.app.container import Container
from qdarchive_seeding.app.progress import (
    AssetDownloadUpdate,
    Completed,
    CountersUpdated,
    ErrorEvent,
    StageChanged,
)
from qdarchive_seeding.core.constants import (
    DOWNLOAD_STATUS_FAILED,
    DOWNLOAD_STATUS_SKIPPED,
    DOWNLOAD_STATUS_SUCCESS,
)
from qdarchive_seeding.core.entities import RunInfo

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ConcreteRunContext:
    run_id: str
    pipeline_id: str
    config: PipelineConfig
    cancelled: threading.Event = field(default_factory=threading.Event)
    metadata: dict[str, Any] = field(default_factory=dict)


class ETLRunner:
    def __init__(self, container: Container) -> None:
        self._c = container

    def run(self, *, dry_run: bool = False) -> RunInfo:
        c = self._c
        run_id = c.run_id
        log = c.logger_bundle.logger
        bus = c.progress_bus

        ctx = ConcreteRunContext(
            run_id=run_id,
            pipeline_id=c.config.pipeline.id,
            config=c.config,
        )

        started_at = datetime.now(UTC)
        extracted = 0
        transformed = 0
        downloaded = 0
        failed = 0
        skipped = 0
        failures: list[dict[str, Any]] = []

        # --- Extract ---
        bus.publish(StageChanged("extract"))
        log.info("Starting extraction")
        try:
            records = list(c.extractor.extract(ctx))
        except Exception as exc:
            log.error("Extraction failed: %s", exc)
            bus.publish(
                ErrorEvent(component="extractor", error_type=type(exc).__name__, message=str(exc))
            )
            records = []
        extracted = len(records)
        bus.publish(CountersUpdated(extracted=extracted))
        log.info("Extracted %d records", extracted)

        # --- Transform ---
        bus.publish(StageChanged("transform"))
        log.info("Starting transforms")
        records = c.transform_chain.run(records)

        max_items = c.config.pipeline.max_items
        if max_items is not None and len(records) > max_items:
            records = records[:max_items]

        transformed = len(records)
        bus.publish(CountersUpdated(extracted=extracted, transformed=transformed))
        log.info("Transformed to %d records", transformed)

        # --- Download + Load ---
        downloads_root = Path(c.config.storage.downloads_root)

        for record in records:
            if ctx.cancelled.is_set():
                log.warning("Run cancelled")
                break

            c.rate_limiter.wait()

            dataset_slug = (record.raw.get("dataset_slug") if record.raw else None) or "dataset"

            target_dir = c.path_strategy.dataset_dir(
                downloads_root,
                source_name=record.source_name,
                dataset_slug=dataset_slug,
            )
            c.filesystem.ensure_dir(target_dir)

            # Download assets
            bus.publish(StageChanged("download"))
            for asset in record.assets:
                if ctx.cancelled.is_set():
                    break
                if c.policy.should_skip_asset(asset):
                    skipped += 1
                    asset.download_status = DOWNLOAD_STATUS_SKIPPED
                    continue
                if dry_run:
                    skipped += 1
                    asset.download_status = DOWNLOAD_STATUS_SKIPPED
                    continue
                try:
                    result = c.downloader.download(asset, target_dir)
                    bus.publish(
                        AssetDownloadUpdate(
                            asset_url=asset.asset_url,
                            status=result.asset.download_status or DOWNLOAD_STATUS_SUCCESS,
                            bytes_downloaded=result.bytes_downloaded,
                        )
                    )
                    if result.asset.download_status == DOWNLOAD_STATUS_SUCCESS:
                        downloaded += 1
                    else:
                        failed += 1
                        failures.append(
                            {"asset_url": asset.asset_url, "error": "non-success status"}
                        )
                except Exception as exc:
                    failed += 1
                    asset.download_status = DOWNLOAD_STATUS_FAILED
                    asset.error_message = str(exc)
                    failures.append({"asset_url": asset.asset_url, "error": str(exc)})
                    bus.publish(
                        ErrorEvent(
                            component="downloader",
                            error_type=type(exc).__name__,
                            message=str(exc),
                            asset_url=asset.asset_url,
                        )
                    )
                    log.error("Download failed for %s: %s", asset.asset_url, exc)

            # Load into sink
            bus.publish(StageChanged("load"))
            try:
                dataset_id = c.sink.upsert_dataset(record)
                for asset in record.assets:
                    c.sink.upsert_asset(dataset_id, asset)
            except Exception as exc:
                log.error("Sink error for record %s: %s", record.source_dataset_id, exc)
                bus.publish(
                    ErrorEvent(
                        component="sink",
                        error_type=type(exc).__name__,
                        message=str(exc),
                    )
                )

            bus.publish(
                CountersUpdated(
                    extracted=extracted,
                    transformed=transformed,
                    downloaded=downloaded,
                    failed=failed,
                    skipped=skipped,
                )
            )

        # --- Complete ---
        ended_at = datetime.now(UTC)
        run_info = RunInfo(
            run_id=ctx.run_id,  # type: ignore[arg-type]
            pipeline_id=ctx.pipeline_id,
            started_at=started_at,
            ended_at=ended_at,
            config_hash=c.config_hash,
            counts={
                "extracted": extracted,
                "transformed": transformed,
                "downloaded": downloaded,
                "failed": failed,
                "skipped": skipped,
            },
            failures=failures,
            environment={
                "python": sys.version,
                "platform": platform.platform(),
            },
        )
        c.manifests.write(run_info)
        bus.publish(StageChanged("done"))
        bus.publish(Completed(run_info=run_info))
        log.info("Run completed: %s", run_info.counts)
        return run_info
