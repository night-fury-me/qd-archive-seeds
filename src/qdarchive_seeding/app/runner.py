from __future__ import annotations

import logging
import platform
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from qdarchive_seeding.app.config_models import PipelineConfig
from qdarchive_seeding.app.container import Container
from qdarchive_seeding.app.progress import (
    AssetDownloadProgress,
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
    cancelled: bool = False
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
        loaded = 0
        failed = 0
        skipped = 0
        failures: list[dict[str, Any]] = []

        # --- Streaming extract → transform → download → load ---
        # Each dataset is downloaded and loaded as soon as it passes
        # pre-transforms, so the user sees progress immediately instead
        # of waiting for all pages to be fetched first.
        bus.publish(StageChanged("extract"))
        log.info("Starting extraction (source=%s)", c.config.source.name)

        max_items = c.config.pipeline.max_items
        downloads_root = Path(c.config.storage.downloads_root)
        total_assets = 0

        # Wire downloader progress callback to the bus
        _current_asset_url = ""

        def _on_download_progress(bytes_so_far: int, total_bytes: int | None) -> None:
            bus.publish(
                AssetDownloadProgress(
                    asset_url=_current_asset_url,
                    bytes_downloaded=bytes_so_far,
                    total_bytes=total_bytes,
                )
            )

        c.downloader.on_progress = _on_download_progress

        try:
            for raw_record in c.extractor.extract(ctx):
                if ctx.cancelled:
                    log.warning("Run cancelled")
                    break

                extracted += 1

                # --- Pre-transform (filter) ---
                result = c.pre_transform_chain.run([raw_record])
                if not result:
                    if extracted % 50 == 0:
                        bus.publish(CountersUpdated(extracted=extracted, transformed=transformed))
                    continue

                record = result[0]
                transformed += 1
                total_assets += len(record.assets)
                log.info(
                    "Dataset %d matched: %s (%d assets)",
                    transformed,
                    record.title or record.source_dataset_id,
                    len(record.assets),
                )
                bus.publish(
                    CountersUpdated(
                        extracted=extracted,
                        transformed=transformed,
                        total_assets=total_assets,
                    )
                )

                # --- Download assets immediately ---
                c.rate_limiter.wait()

                dataset_slug = (record.raw.get("dataset_slug") if record.raw else None) or "dataset"
                target_dir = c.path_strategy.dataset_dir(
                    downloads_root,
                    source_name=record.source_name,
                    dataset_slug=dataset_slug,
                )
                c.filesystem.ensure_dir(target_dir)

                for asset in record.assets:
                    if ctx.cancelled:
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
                        _current_asset_url = asset.asset_url
                        dl_result = c.downloader.download(asset, target_dir)
                        bus.publish(
                            AssetDownloadUpdate(
                                asset_url=asset.asset_url,
                                status=dl_result.asset.download_status or DOWNLOAD_STATUS_SUCCESS,
                                bytes_downloaded=dl_result.bytes_downloaded,
                            )
                        )
                        if dl_result.asset.download_status == DOWNLOAD_STATUS_SUCCESS:
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

                # --- Post-transform + Load ---
                post_result = c.post_transform_chain.run([record])
                if not post_result:
                    continue

                try:
                    dataset_id = c.sink.upsert_dataset(record)
                    for asset in record.assets:
                        c.sink.upsert_asset(dataset_id, asset)
                    loaded += 1
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
                        total_assets=total_assets,
                    )
                )

                # Stop once we have enough filtered datasets
                if max_items is not None and transformed >= max_items:
                    log.info(
                        "Reached max_items=%d filtered datasets (extracted %d raw)",
                        max_items,
                        extracted,
                    )
                    break

        except Exception as exc:
            log.error("Extraction failed: %s", exc)
            bus.publish(
                ErrorEvent(component="extractor", error_type=type(exc).__name__, message=str(exc))
            )

        log.info("Extracted %d raw, %d passed filters", extracted, transformed)

        # --- Complete ---
        ended_at = datetime.now(UTC)
        run_info = RunInfo(
            run_id=ctx.run_id,
            pipeline_id=ctx.pipeline_id,
            started_at=started_at,
            ended_at=ended_at,
            config_hash=c.config_hash,
            counts={
                "extracted": extracted,
                "transformed": transformed,
                "downloaded": downloaded,
                "loaded": loaded,
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
