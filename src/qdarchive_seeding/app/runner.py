from __future__ import annotations

import logging
import platform
import sys
from collections.abc import Callable
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
    MetadataCollected,
    StageChanged,
)
from qdarchive_seeding.core.constants import (
    DOWNLOAD_RESULT_UNKNOWN,
    DOWNLOAD_STATUS_FAILED,
    DOWNLOAD_STATUS_SKIPPED,
    DOWNLOAD_STATUS_SUCCESS,
)
from qdarchive_seeding.core.entities import DatasetRecord, RunInfo

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ConcreteRunContext:
    run_id: str
    pipeline_id: str
    config: PipelineConfig
    cancelled: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class DownloadDecision:
    """Controls what fraction of collected datasets to download."""

    download_all: bool = True
    percentage: int = 100  # 1-100


class ETLRunner:
    def __init__(self, container: Container) -> None:
        self._c = container

    def run(
        self,
        *,
        dry_run: bool = False,
        no_confirm: bool = False,
        metadata_only: bool = False,
        download_decision: DownloadDecision | None = None,
        confirm_callback: Callable[[int, int, int], DownloadDecision] | None = None,
    ) -> RunInfo:
        c = self._c
        run_id = c.run_id
        log = c.logger_bundle.logger
        bus = c.progress_bus

        ctx = ConcreteRunContext(
            run_id=run_id,
            pipeline_id=c.config.pipeline.id,
            config=c.config,
            metadata={"progress_bus": bus},
        )

        started_at = datetime.now(UTC)
        extracted = 0
        transformed = 0
        downloaded = 0
        loaded = 0
        failed = 0
        skipped = 0
        failures: list[dict[str, Any]] = []
        total_assets = 0

        phases = c.config.pipeline.phases
        collected_records: list[DatasetRecord] = []
        collected_dataset_ids: list[str] = []

        # ===== Phase 1: Metadata Collection =====
        if "metadata" in phases:
            bus.publish(StageChanged("metadata_collection"))
            log.info("Phase 1: Collecting metadata (source=%s)", c.config.source.name)

            max_items = c.config.pipeline.max_items

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
                            bus.publish(
                                CountersUpdated(extracted=extracted, transformed=transformed)
                            )
                        continue

                    record = result[0]
                    transformed += 1
                    total_assets += len(record.assets)
                    bus.publish(
                        CountersUpdated(
                            extracted=extracted,
                            transformed=transformed,
                            total_assets=total_assets,
                        )
                    )

                    # --- Post-transform ---
                    post_result = c.post_transform_chain.run([record])
                    if not post_result:
                        continue
                    record = post_result[0]

                    # --- Store metadata (no download yet) ---
                    for asset in record.assets:
                        asset.download_status = DOWNLOAD_RESULT_UNKNOWN

                    try:
                        dataset_id = c.sink.upsert_dataset(record)
                        for asset in record.assets:
                            c.sink.upsert_asset(dataset_id, asset)
                        loaded += 1
                        collected_records.append(record)
                        collected_dataset_ids.append(dataset_id)
                    except Exception as exc:
                        log.error(
                            "Sink error for record %s: %s", record.source_dataset_id, exc
                        )
                        bus.publish(
                            ErrorEvent(
                                component="sink",
                                error_type=type(exc).__name__,
                                message=str(exc),
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
                    ErrorEvent(
                        component="extractor",
                        error_type=type(exc).__name__,
                        message=str(exc),
                    )
                )

            # Compute total estimated size from asset metadata
            total_size_bytes = sum(
                asset.size_bytes or 0
                for record in collected_records
                for asset in record.assets
            )

            log.info(
                "Phase 1 complete: %d datasets, %d files, %.1f MB",
                loaded,
                total_assets,
                total_size_bytes / (1024 * 1024),
            )
            bus.publish(
                MetadataCollected(
                    total_projects=loaded,
                    total_files=total_assets,
                    total_size_bytes=total_size_bytes,
                )
            )

        # ===== Phase 2: Download =====
        if "download" in phases and not metadata_only and not dry_run:
            # Get download decision: callback (interactive) > explicit > default
            if confirm_callback is not None and not no_confirm and loaded > 0:
                decision = confirm_callback(loaded, total_assets, total_size_bytes)
            else:
                decision = download_decision or DownloadDecision()

            # User chose to skip download
            if decision.percentage == 0:
                log.info("Download skipped by user")
                skipped = total_assets
            elif collected_records:
                bus.publish(StageChanged("download"))
                log.info("Phase 2: Downloading assets for %d datasets", len(collected_records))

                downloads_root = Path(c.config.storage.downloads_root)
                if not decision.download_all and decision.percentage < 100:
                    limit = max(1, len(collected_records) * decision.percentage // 100)
                    records_to_download = collected_records[:limit]
                    dataset_ids_to_download = collected_dataset_ids[:limit]
                    log.info(
                        "Downloading %d/%d datasets (%d%%)",
                        limit,
                        len(collected_records),
                        decision.percentage,
                    )
                else:
                    records_to_download = collected_records
                    dataset_ids_to_download = collected_dataset_ids

                # Wire downloader progress callback to the bus
                _current_asset_url = ""

                def _on_download_progress(
                    bytes_so_far: int, total_bytes: int | None
                ) -> None:
                    bus.publish(
                        AssetDownloadProgress(
                            asset_url=_current_asset_url,
                            bytes_downloaded=bytes_so_far,
                            total_bytes=total_bytes,
                        )
                    )

                c.downloader.on_progress = _on_download_progress

                for record, dataset_id in zip(
                    records_to_download, dataset_ids_to_download, strict=True
                ):
                    if ctx.cancelled:
                        log.warning("Run cancelled during download")
                        break

                    # Restore prior download statuses from DB so the policy
                    # can skip already-downloaded files on resume.
                    if hasattr(c.sink, "get_file_statuses"):
                        prior_statuses = c.sink.get_file_statuses(dataset_id)
                        for asset in record.assets:
                            fname = (
                                asset.local_filename
                                or asset.asset_url.rsplit("/", 1)[-1]
                            )
                            if fname in prior_statuses:
                                asset.download_status = prior_statuses[fname]

                    c.rate_limiter.wait()

                    dataset_slug = (
                        (record.raw.get("dataset_slug") if record.raw else None)
                        or "dataset"
                    )
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
                        try:
                            _current_asset_url = asset.asset_url
                            dl_result = c.downloader.download(asset, target_dir)
                            bus.publish(
                                AssetDownloadUpdate(
                                    asset_url=asset.asset_url,
                                    status=dl_result.asset.download_status
                                    or DOWNLOAD_STATUS_SUCCESS,
                                    bytes_downloaded=dl_result.bytes_downloaded,
                                )
                            )
                            if dl_result.asset.download_status == DOWNLOAD_STATUS_SUCCESS:
                                downloaded += 1
                            else:
                                failed += 1
                                failures.append(
                                    {
                                        "asset_url": asset.asset_url,
                                        "error": "non-success status",
                                    }
                                )
                        except Exception as exc:
                            failed += 1
                            asset.download_status = DOWNLOAD_STATUS_FAILED
                            asset.error_message = str(exc)
                            failures.append(
                                {"asset_url": asset.asset_url, "error": str(exc)}
                            )
                            bus.publish(
                                ErrorEvent(
                                    component="downloader",
                                    error_type=type(exc).__name__,
                                    message=str(exc),
                                    asset_url=asset.asset_url,
                                )
                            )
                            log.error(
                                "Download failed for %s: %s", asset.asset_url, exc
                            )

                    # Update sink with download results
                    try:
                        for asset in record.assets:
                            c.sink.upsert_asset(dataset_id, asset)
                    except Exception as exc:
                        log.error("Sink update error for %s: %s", dataset_id, exc)

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
        elif dry_run:
            skipped = total_assets

        c.sink.close()

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
