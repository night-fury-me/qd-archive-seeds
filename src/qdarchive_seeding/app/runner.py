"""Pipeline orchestrator — delegates download logic to asset_processing module."""

from __future__ import annotations

import asyncio
import logging
import platform
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from qdarchive_seeding.app.asset_processing import process_dataset
from qdarchive_seeding.app.config_models import PipelineConfig
from qdarchive_seeding.app.container import Container
from qdarchive_seeding.app.download_strategy import (
    DownloadConfig,
    DownloadCounters,
    DownloadCtx,
    DownloadDecision,
    DownloadState,
)
from qdarchive_seeding.core.constants import DOWNLOAD_STATUS_UNKNOWN
from qdarchive_seeding.core.entities import DatasetRecord, RunInfo
from qdarchive_seeding.core.interfaces import Checkpoint, ResumableSink
from qdarchive_seeding.core.interfaces import ProgressBus as ProgressBusProto
from qdarchive_seeding.core.progress import (
    Completed,
    CountersUpdated,
    ErrorEvent,
    MetadataCollected,
    StageChanged,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ConcreteRunContext:
    run_id: str
    pipeline_id: str
    config: PipelineConfig
    cancelled: bool = False
    progress_bus: ProgressBusProto | None = None
    checkpoint: Checkpoint | None = None
    existing_dataset_ids: set[str] | None = None


class ETLRunner:
    def __init__(self, container: Container) -> None:
        self._c = container
        self._icpsr_cookie_cache: dict[str, dict[str, str]] = {}

    async def run(
        self,
        *,
        dry_run: bool = False,
        no_confirm: bool = False,
        metadata_only: bool = False,
        fresh_extract: bool = False,
        download_decision: DownloadDecision | None = None,
        confirm_callback: Callable[[int, int, int], DownloadDecision] | None = None,
        icpsr_confirm_callback: Callable[[int], bool] | None = None,
        icpsr_terms_url_callback: Callable[[str], None] | None = None,
    ) -> RunInfo:
        c = self._c
        run_id = c.run_id
        log = c.logger_bundle.logger
        bus = c.progress_bus

        ctx = ConcreteRunContext(
            run_id=run_id,
            pipeline_id=c.config.pipeline.id,
            config=c.config,
            progress_bus=bus,
            checkpoint=c.checkpoint,
        )

        started_at = datetime.now(UTC)
        extracted = 0
        transformed = 0
        counters = DownloadCounters()
        loaded = 0
        total_assets = 0

        phases = c.config.pipeline.phases
        collected_records: list[DatasetRecord] = []
        collected_dataset_ids: list[str] = []
        collected_ids_set: set[str] = set()

        # ===== Phase 1: Metadata Collection =====
        if "metadata" in phases:
            (
                extracted,
                transformed,
                loaded,
                total_assets,
            ) = await self._run_metadata_phase(
                ctx, c, bus, log, collected_records, collected_dataset_ids, collected_ids_set
            )

        # ===== Phase 2: Download =====
        if "download" in phases and not metadata_only and not dry_run:
            await self._run_download_phase(
                ctx,
                c,
                bus,
                log,
                counters,
                collected_records,
                collected_dataset_ids,
                collected_ids_set,
                extracted,
                transformed,
                total_assets,
                no_confirm=no_confirm,
                download_decision=download_decision,
                confirm_callback=confirm_callback,
                icpsr_confirm_callback=icpsr_confirm_callback,
                icpsr_terms_url_callback=icpsr_terms_url_callback,
            )
        elif dry_run:
            counters.skipped = total_assets

        # --- Cleanup & Complete ---
        try:
            c.sink.close()
            await c.close()
        except Exception as cleanup_exc:
            log.warning("Cleanup error: %s", cleanup_exc)

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
                "downloaded": counters.downloaded,
                "loaded": loaded,
                "failed": counters.failed,
                "skipped": counters.skipped,
            },
            failures=counters.failures,
            environment={
                "python": sys.version,
                "platform": platform.platform(),
            },
        )
        c.manifests.write(run_info)
        if c.checkpoint.has_unresolved_failures():
            log.warning("Checkpoint retained: unresolved extraction page failures remain")
        else:
            completed_count = c.checkpoint.completed_count()
            log.info(
                "Checkpoint preserved: %d completed queries available for skip on next run",
                completed_count,
            )
        bus.publish(StageChanged("done"))
        bus.publish(Completed(run_info=run_info))
        log.info("Run completed: %s", run_info.counts)
        return run_info

    # ------------------------------------------------------------------
    # Phase 1: Metadata Collection
    # ------------------------------------------------------------------

    async def _run_metadata_phase(
        self,
        ctx: ConcreteRunContext,
        c: Container,
        bus: Any,
        log: logging.Logger,
        collected_records: list[DatasetRecord],
        collected_dataset_ids: list[str],
        collected_ids_set: set[str],
    ) -> tuple[int, int, int, int]:
        """Extract, transform, and store metadata.

        Returns (extracted, transformed, loaded, total_assets).
        """
        extracted = 0
        transformed = 0
        loaded = 0
        total_assets = 0

        bus.publish(StageChanged("metadata_collection"))
        log.info("Phase 1: Collecting metadata (source=%s)", c.config.source.name)

        # Pre-populate seen_ids from sink for resume support
        if isinstance(c.sink, ResumableSink) and c.config.source.repository_id:
            existing_ids = c.sink.get_existing_dataset_ids(c.config.source.repository_id)
            if existing_ids:
                log.info("Resume: loaded %d existing dataset IDs from sink", len(existing_ids))
                ctx.existing_dataset_ids = existing_ids

        max_items = c.config.pipeline.max_items

        try:
            async for raw_record in c.extractor.extract(ctx):
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
                    asset.download_status = DOWNLOAD_STATUS_UNKNOWN

                try:
                    dataset_id = c.sink.upsert_dataset(record)
                    for asset in record.assets:
                        c.sink.upsert_asset(dataset_id, asset)
                    loaded += 1
                    if dataset_id not in collected_ids_set:
                        collected_records.append(record)
                        collected_dataset_ids.append(dataset_id)
                        collected_ids_set.add(dataset_id)
                except Exception as exc:
                    log.error("Sink error for record %s: %s", record.source_dataset_id, exc)
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
            asset.size_bytes or 0 for record in collected_records for asset in record.assets
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

        return extracted, transformed, loaded, total_assets

    # ------------------------------------------------------------------
    # Phase 2: Download
    # ------------------------------------------------------------------

    async def _run_download_phase(
        self,
        ctx: ConcreteRunContext,
        c: Container,
        bus: Any,
        log: logging.Logger,
        counters: DownloadCounters,
        collected_records: list[DatasetRecord],
        collected_dataset_ids: list[str],
        collected_ids_set: set[str],
        extracted: int,
        transformed: int,
        total_assets: int,
        *,
        no_confirm: bool,
        download_decision: DownloadDecision | None,
        confirm_callback: Callable[[int, int, int], DownloadDecision] | None,
        icpsr_confirm_callback: Callable[[int], bool] | None,
        icpsr_terms_url_callback: Callable[[str], None] | None,
    ) -> None:
        """Determine download scope and execute downloads."""
        # Load previously-extracted datasets from DB that still need downloading
        if isinstance(c.sink, ResumableSink):
            pending = c.sink.get_pending_download_datasets(
                repository_id=c.config.source.repository_id
            )
            # Merge: add DB records not already in collected_records
            for db_id, db_record, _assets in pending:
                if db_id not in collected_ids_set:
                    collected_records.append(db_record)
                    collected_dataset_ids.append(db_id)
                    collected_ids_set.add(db_id)
                    total_assets += len(db_record.assets)
            if pending:
                # Recompute total size with merged records
                total_size_bytes = sum(
                    asset.size_bytes or 0 for record in collected_records for asset in record.assets
                )
                log.info(
                    "Loaded %d pending-download datasets from DB "
                    "(total now: %d datasets, %d files)",
                    len(pending) - len(collected_ids_set & {p[0] for p in pending}),
                    len(collected_records),
                    total_assets,
                )

        # Recompute total_size_bytes for decision callback
        total_size_bytes = sum(
            asset.size_bytes or 0 for record in collected_records for asset in record.assets
        )

        # Get download decision: callback (interactive) > explicit > default
        if confirm_callback is not None and not no_confirm and collected_records:
            decision = confirm_callback(len(collected_records), total_assets, total_size_bytes)
        else:
            decision = download_decision or DownloadDecision()

        # User chose to skip download
        if decision.percentage == 0 and decision.exact_count is None:
            log.info("Download skipped by user")
            counters.skipped = total_assets
            return

        if not collected_records:
            return

        downloads_root = Path(c.config.storage.downloads_root)
        # Track assets excluded before the download phase separately
        pre_excluded = 0
        if decision.exact_count is not None:
            limit = max(1, min(len(collected_records), decision.exact_count))
            records_to_download = collected_records[:limit]
            dataset_ids_to_download = collected_dataset_ids[:limit]
            pre_excluded = sum(len(r.assets) for r in collected_records[limit:])
            counters.skipped += pre_excluded
        elif not decision.download_all and decision.percentage < 100:
            limit = max(1, len(collected_records) * decision.percentage // 100)
            records_to_download = collected_records[:limit]
            dataset_ids_to_download = collected_dataset_ids[:limit]
            pre_excluded = sum(len(r.assets) for r in collected_records[limit:])
            counters.skipped += pre_excluded
        else:
            records_to_download = collected_records
            dataset_ids_to_download = collected_dataset_ids

        # Check for ICPSR datasets and prompt user if callback provided
        skip_icpsr = False
        icpsr_count = sum(
            1
            for r in records_to_download
            for a in r.assets
            if "openicpsr.org" in a.asset_url or "icpsr.umich.edu" in a.asset_url
        )
        if (
            icpsr_count > 0
            and icpsr_confirm_callback is not None
            and not icpsr_confirm_callback(icpsr_count)
        ):
            skip_icpsr = True
            log.info("User skipped ICPSR downloads (%d assets)", icpsr_count)

        # Publish stage change and counters after all prompts are done
        bus.publish(StageChanged("download"))

        # Compute asset count for the download subset
        download_asset_count = sum(len(r.assets) for r in records_to_download)
        log.info(
            "Phase 2: Downloading %d assets for %d datasets",
            download_asset_count,
            len(records_to_download),
        )
        bus.publish(
            CountersUpdated(
                extracted=extracted,
                transformed=transformed,
                total_assets=download_asset_count,
            )
        )

        # Global semaphore for concurrent downloads across all datasets
        download_sem = asyncio.Semaphore(5)

        dctx = DownloadCtx(
            cfg=DownloadConfig(
                ctx=ctx,
                container=c,
                bus=bus,
                log=log,
                skip_icpsr=skip_icpsr,
                icpsr_terms_url_callback=icpsr_terms_url_callback,
                icpsr_prompt_lock=asyncio.Lock(),
                download_sem=download_sem,
                downloads_root=downloads_root,
                extracted=extracted,
                transformed=transformed,
                pre_excluded=pre_excluded,
            ),
            state=DownloadState(
                counters=counters,
                download_asset_count=download_asset_count,
            ),
        )

        # Process datasets with limited concurrency
        dataset_sem = asyncio.Semaphore(5)
        cookie_cache = self._icpsr_cookie_cache

        async def _throttled_dataset(record: Any, dataset_id: str) -> None:
            async with dataset_sem:
                await process_dataset(dctx, record, dataset_id, cookie_cache)

        dataset_tasks = [
            _throttled_dataset(record, dataset_id)
            for record, dataset_id in zip(records_to_download, dataset_ids_to_download, strict=True)
        ]
        await asyncio.gather(*dataset_tasks)
        if counters.access_denied > 0:
            log.info(
                "Suppressed %d download errors (access denied / network)",
                counters.access_denied,
            )
