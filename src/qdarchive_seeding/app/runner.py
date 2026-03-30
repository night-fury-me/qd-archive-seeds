from __future__ import annotations

import asyncio
import contextlib
import logging
import platform
import sys
import zipfile
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
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
    exact_count: int | None = None  # if set, download exactly this many datasets


_icpsr_cookie_cache: dict[str, str] | None = None


def _get_icpsr_browser_cookies(config: PipelineConfig) -> dict[str, str]:
    """Extract and cache ICPSR browser cookies for Classic ICPSR downloads."""
    global _icpsr_cookie_cache  # noqa: PLW0603
    if _icpsr_cookie_cache is not None:
        return _icpsr_cookie_cache

    ext_auth = config.external_auth.get("www.icpsr.umich.edu")
    if ext_auth is None or ext_auth.type != "browser_session":
        _icpsr_cookie_cache = {}
        return _icpsr_cookie_cache

    try:
        import browser_cookie3  # type: ignore[import-untyped]

        loader = {"chromium": browser_cookie3.chromium, "chrome": browser_cookie3.chrome}.get(
            ext_auth.browser, browser_cookie3.chromium
        )
        cookie_jar = loader(domain_name=".icpsr.umich.edu")
        _icpsr_cookie_cache = {
            c.name: c.value
            for c in cookie_jar
            if c.value and not c.name.lower().startswith(("__cf", "cf_", "_cf"))
        }
    except Exception:
        _icpsr_cookie_cache = {}
        logger.warning("Failed to extract ICPSR browser cookies")

    return _icpsr_cookie_cache


def _extract_zip_bundle(
    zip_path: Path,
    target_dir: Path,
    log: logging.Logger,
) -> list[Any]:
    """Extract a ZIP bundle and return AssetRecords for individual files."""
    from qdarchive_seeding.core.entities import AssetRecord

    if not zip_path.exists() or not zipfile.is_zipfile(zip_path):
        log.warning("Not a valid ZIP file: %s", zip_path)
        return []

    extracted: list[AssetRecord] = []
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(target_dir)

            # Detect single top-level directory and flatten it
            top_level_dirs = {
                PurePosixPath(info.filename).parts[0]
                for info in zf.infolist()
                if len(PurePosixPath(info.filename).parts) > 1
            }
            single_subdir = (
                (target_dir / next(iter(top_level_dirs))) if len(top_level_dirs) == 1 else None
            )
            if single_subdir and single_subdir.is_dir():
                import shutil

                for child in list(single_subdir.iterdir()):
                    dest = target_dir / child.name
                    if dest.exists():
                        # Merge: if both are dirs, move contents; otherwise skip
                        if child.is_dir() and dest.is_dir():
                            for sub in child.rglob("*"):
                                if sub.is_file():
                                    rel = sub.relative_to(child)
                                    sub_dest = dest / rel
                                    sub_dest.parent.mkdir(parents=True, exist_ok=True)
                                    if not sub_dest.exists():
                                        shutil.move(str(sub), str(sub_dest))
                            shutil.rmtree(child)
                        # else: destination file already exists, skip
                    else:
                        shutil.move(str(child), str(dest))
                # Remove subdir if now empty
                with contextlib.suppress(OSError):
                    single_subdir.rmdir()
                log.debug("Flattened single top-level directory: %s", single_subdir.name)

            for info in zf.infolist():
                if info.is_dir():
                    continue
                # Strip the top-level directory prefix if we flattened
                filename = info.filename
                if single_subdir is not None:
                    parts = PurePosixPath(filename).parts
                    filename = str(PurePosixPath(*parts[1:])) if len(parts) > 1 else filename
                file_path = target_dir / filename
                file_type = PurePosixPath(filename).suffix.lstrip(".") or None
                extracted.append(
                    AssetRecord(
                        asset_url=zip_path.name,
                        local_dir=str(file_path.parent),
                        local_filename=file_path.name,
                        file_type=file_type,
                        size_bytes=info.file_size,
                        download_status=DOWNLOAD_STATUS_SUCCESS,
                        downloaded_at=datetime.now(UTC),
                    )
                )
        # Remove the ZIP after successful extraction
        zip_path.unlink()
        log.info("Extracted %d files from %s", len(extracted), zip_path.name)
    except Exception as exc:
        log.error("Failed to extract ZIP %s: %s", zip_path, exc)
        return []
    return extracted


class ETLRunner:
    def __init__(self, container: Container) -> None:
        self._c = container

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
    ) -> RunInfo:
        c = self._c
        run_id = c.run_id
        log = c.logger_bundle.logger
        bus = c.progress_bus

        ctx = ConcreteRunContext(
            run_id=run_id,
            pipeline_id=c.config.pipeline.id,
            config=c.config,
            metadata={"progress_bus": bus, "checkpoint": c.checkpoint},
        )

        started_at = datetime.now(UTC)
        extracted = 0
        transformed = 0
        downloaded = 0
        loaded = 0
        failed = 0
        skipped = 0
        access_denied = 0
        failures: list[dict[str, Any]] = []
        total_assets = 0

        phases = c.config.pipeline.phases
        collected_records: list[DatasetRecord] = []
        collected_dataset_ids: list[str] = []

        # ===== Phase 1: Metadata Collection =====
        if "metadata" in phases:
            bus.publish(StageChanged("metadata_collection"))
            log.info("Phase 1: Collecting metadata (source=%s)", c.config.source.name)

            # Pre-populate seen_ids from sink for resume support
            if hasattr(c.sink, "get_existing_dataset_ids") and c.config.source.repository_id:
                existing_ids = c.sink.get_existing_dataset_ids(c.config.source.repository_id)
                if existing_ids:
                    log.info("Resume: loaded %d existing dataset IDs from sink", len(existing_ids))
                    ctx.metadata["existing_dataset_ids"] = existing_ids

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

        # ===== Phase 2: Download =====
        if "download" in phases and not metadata_only and not dry_run:
            # Load previously-extracted datasets from DB that still need downloading
            if hasattr(c.sink, "get_pending_download_datasets"):
                pending = c.sink.get_pending_download_datasets(
                    repository_id=c.config.source.repository_id
                )
                # Merge: add DB records not already in collected_records
                collected_ids_set = set(collected_dataset_ids)
                for db_id, db_record, _assets in pending:
                    if db_id not in collected_ids_set:
                        collected_records.append(db_record)
                        collected_dataset_ids.append(db_id)
                        total_assets += len(db_record.assets)
                if pending:
                    # Recompute total size with merged records
                    total_size_bytes = sum(
                        asset.size_bytes or 0
                        for record in collected_records
                        for asset in record.assets
                    )
                    log.info(
                        "Loaded %d pending-download datasets from DB "
                        "(total now: %d datasets, %d files)",
                        len(pending) - len(collected_ids_set & {p[0] for p in pending}),
                        len(collected_records),
                        total_assets,
                    )

            # Get download decision: callback (interactive) > explicit > default
            if confirm_callback is not None and not no_confirm and collected_records:
                decision = confirm_callback(len(collected_records), total_assets, total_size_bytes)
            else:
                decision = download_decision or DownloadDecision()

            # User chose to skip download
            if decision.percentage == 0 and decision.exact_count is None:
                log.info("Download skipped by user")
                skipped = total_assets
            elif collected_records:
                downloads_root = Path(c.config.storage.downloads_root)
                # Track assets excluded before the download phase separately
                # so they don't inflate the progress bar's completed count.
                pre_excluded = 0
                if decision.exact_count is not None:
                    limit = max(1, min(len(collected_records), decision.exact_count))
                    records_to_download = collected_records[:limit]
                    dataset_ids_to_download = collected_dataset_ids[:limit]
                    pre_excluded = sum(len(r.assets) for r in collected_records[limit:])
                    skipped += pre_excluded
                elif not decision.download_all and decision.percentage < 100:
                    limit = max(1, len(collected_records) * decision.percentage // 100)
                    records_to_download = collected_records[:limit]
                    dataset_ids_to_download = collected_dataset_ids[:limit]
                    pre_excluded = sum(len(r.assets) for r in collected_records[limit:])
                    skipped += pre_excluded
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

                _access_codes = (
                    "403",
                    "401",
                    "400",
                    "404",
                    "Forbidden",
                    "Unauthorized",
                    "Bad Request",
                    "Not Found",
                    "login page",
                )
                _transient_codes = (
                    "name resolution",
                    "timed out",
                    "ConnectError",
                    "RemoteProtocolError",
                    "Server disconnected",
                )

                # Concurrency lock for shared counter updates
                _counter_lock = asyncio.Lock()

                async def _publish_progress() -> None:
                    """Publish current counter state to the progress bus."""
                    bus.publish(
                        CountersUpdated(
                            extracted=extracted,
                            transformed=transformed,
                            downloaded=downloaded,
                            failed=failed,
                            skipped=skipped - pre_excluded,
                            access_denied=access_denied,
                            total_assets=download_asset_count,
                        )
                    )

                async def _download_one(
                    asset_ref: Any,
                    target: Path,
                    sem: asyncio.Semaphore,
                ) -> tuple[Any, Any | None, Exception | None]:
                    nonlocal downloaded, failed, skipped, access_denied

                    async with sem:
                        if ctx.cancelled:
                            return asset_ref, None, None
                        if c.policy.should_skip_asset(asset_ref):
                            asset_ref.download_status = DOWNLOAD_STATUS_SKIPPED
                            async with _counter_lock:
                                skipped += 1
                                await _publish_progress()
                            return asset_ref, None, None
                        # Skip ICPSR assets if user declined
                        if skip_icpsr and (
                            "openicpsr.org" in asset_ref.asset_url
                            or "icpsr.umich.edu" in asset_ref.asset_url
                        ):
                            asset_ref.download_status = DOWNLOAD_STATUS_SKIPPED
                            async with _counter_lock:
                                skipped += 1
                                await _publish_progress()
                            return asset_ref, None, None
                        try:
                            # Classic ICPSR: 3-step form flow (sync, run in thread)
                            if "zipcart2" in asset_ref.asset_url:
                                from qdarchive_seeding.infra.storage.icpsr_downloader import (
                                    download_classic_icpsr,
                                )

                                icpsr_cookies = _get_icpsr_browser_cookies(c.config)
                                zip_path = await asyncio.to_thread(
                                    download_classic_icpsr,
                                    asset_ref,
                                    target,
                                    icpsr_cookies,
                                )
                                if zip_path is not None:
                                    asset_ref.download_status = DOWNLOAD_STATUS_SUCCESS
                                    asset_ref.asset_type = "zip_bundle"
                                    from datetime import UTC, datetime

                                    asset_ref.downloaded_at = datetime.now(UTC)
                                    async with _counter_lock:
                                        downloaded += 1
                                        await _publish_progress()
                                else:
                                    asset_ref.download_status = DOWNLOAD_STATUS_FAILED
                                    asset_ref.error_message = "ICPSR form flow failed"
                                    async with _counter_lock:
                                        failed += 1
                                        await _publish_progress()
                                return asset_ref, None, None

                            # Per-asset progress callback avoids shared state
                            def _on_progress(bytes_so_far: int, total_bytes: int | None) -> None:
                                bus.publish(
                                    AssetDownloadProgress(
                                        asset_url=asset_ref.asset_url,
                                        bytes_downloaded=bytes_so_far,
                                        total_bytes=total_bytes,
                                    )
                                )

                            dl_result = await c.downloader.download(
                                asset_ref, target, progress_callback=_on_progress
                            )
                            # Update counters immediately
                            async with _counter_lock:
                                if (
                                    dl_result
                                    and dl_result.asset.download_status == DOWNLOAD_STATUS_SUCCESS
                                ):
                                    downloaded += 1
                                    bus.publish(
                                        AssetDownloadUpdate(
                                            asset_url=asset_ref.asset_url,
                                            status=DOWNLOAD_STATUS_SUCCESS,
                                            bytes_downloaded=dl_result.bytes_downloaded,
                                        )
                                    )
                                else:
                                    failed += 1
                                    failures.append(
                                        {
                                            "asset_url": asset_ref.asset_url,
                                            "error": "non-success status",
                                        }
                                    )
                                await _publish_progress()
                            return asset_ref, dl_result, None
                        except Exception as dl_exc:
                            asset_ref.download_status = DOWNLOAD_STATUS_FAILED
                            asset_ref.error_message = str(dl_exc)
                            err_str = str(dl_exc)
                            async with _counter_lock:
                                failed += 1
                                failures.append(
                                    {"asset_url": asset_ref.asset_url, "error": err_str}
                                )
                                is_suppressed = (
                                    not err_str.strip()
                                    or any(code in err_str for code in _access_codes)
                                    or any(code in err_str for code in _transient_codes)
                                )
                                if is_suppressed:
                                    access_denied += 1
                                else:
                                    bus.publish(
                                        ErrorEvent(
                                            component="downloader",
                                            error_type=type(dl_exc).__name__,
                                            message=err_str,
                                            asset_url=asset_ref.asset_url,
                                        )
                                    )
                                    log.error(
                                        "Download failed for %s: %s",
                                        asset_ref.asset_url,
                                        dl_exc,
                                    )
                                await _publish_progress()
                            return asset_ref, None, dl_exc

                async def _process_dataset(record: Any, dataset_id: str) -> None:
                    nonlocal downloaded

                    if ctx.cancelled:
                        return

                    # Restore prior download statuses from DB so the policy
                    # can skip already-downloaded files on resume.
                    # Prioritize asset_url match (reliable) over file_name (may differ).
                    # Skip UNKNOWN statuses as they indicate unresolved entries.
                    if hasattr(c.sink, "get_file_statuses"):
                        prior_statuses = c.sink.get_file_statuses(dataset_id)
                        for asset in record.assets:
                            fname = asset.local_filename or asset.asset_url.rsplit("/", 1)[-1]
                            status = prior_statuses.get(asset.asset_url) or prior_statuses.get(
                                fname
                            )
                            if status and status != "UNKNOWN":
                                asset.download_status = status

                    dataset_slug = (
                        record.download_project_folder
                        or (record.raw.get("dataset_slug") if record.raw else None)
                        or "dataset"
                    )
                    target_dir = c.path_strategy.dataset_dir(
                        downloads_root,
                        source_name=record.source_name,
                        dataset_slug=dataset_slug,
                    )
                    c.filesystem.ensure_dir(target_dir)

                    tasks = [
                        _download_one(asset, target_dir, download_sem) for asset in record.assets
                    ]
                    # Counter updates happen inside _download_one for real-time progress
                    await asyncio.gather(*tasks)

                    # Extract ZIP bundles and update asset records
                    for asset in list(record.assets):
                        is_zip = asset.asset_type == "zip_bundle" or (
                            asset.local_filename or ""
                        ).lower().endswith(".zip")
                        if (
                            asset.download_status == DOWNLOAD_STATUS_SUCCESS
                            and is_zip
                            and asset.local_dir
                            and asset.local_filename
                        ):
                            zip_files = _extract_zip_bundle(
                                Path(asset.local_dir) / asset.local_filename,
                                Path(asset.local_dir),
                                log,
                            )
                            if zip_files:
                                with contextlib.suppress(Exception):
                                    c.sink.upsert_asset(dataset_id, asset)
                                new_assets: list[Any] = [a for a in record.assets if a is not asset]
                                new_assets.extend(zip_files)
                                record.assets = new_assets
                                async with _counter_lock:
                                    downloaded += len(zip_files) - 1
                                    await _publish_progress()

                    # Update sink with download results (skip policy-skipped assets
                    # to avoid overwriting good DB statuses like SUCCESS with SKIPPED)
                    try:
                        for asset in record.assets:
                            if asset.download_status != DOWNLOAD_STATUS_SKIPPED:
                                c.sink.upsert_asset(dataset_id, asset)
                    except Exception as exc:
                        log.error("Sink update error for %s: %s", dataset_id, exc)

                # Process all datasets concurrently
                dataset_tasks = [
                    _process_dataset(record, dataset_id)
                    for record, dataset_id in zip(
                        records_to_download, dataset_ids_to_download, strict=True
                    )
                ]
                await asyncio.gather(*dataset_tasks)
                if access_denied > 0:
                    log.info(
                        "Suppressed %d download errors (access denied / network)", access_denied
                    )
        elif dry_run:
            skipped = total_assets

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
        if c.checkpoint.has_unresolved_failures():
            log.warning("Checkpoint retained: unresolved extraction page failures remain")
        else:
            completed_count = sum(1 for q in c.checkpoint._queries.values() if q.completed)
            log.info(
                "Checkpoint preserved: %d completed queries available for skip on next run",
                completed_count,
            )
        bus.publish(StageChanged("done"))
        bus.publish(Completed(run_info=run_info))
        log.info("Run completed: %s", run_info.counts)
        return run_info
