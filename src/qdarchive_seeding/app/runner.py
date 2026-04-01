from __future__ import annotations

import asyncio
import contextlib
import logging
import os
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
    AssetDownloadStarted,
    AssetDownloadUpdate,
    Completed,
    CountersUpdated,
    ErrorEvent,
    MetadataCollected,
    StageChanged,
)
from qdarchive_seeding.core.constants import (
    DOWNLOAD_STATUS_FAILED,
    DOWNLOAD_STATUS_SKIPPED,
    DOWNLOAD_STATUS_SUCCESS,
    DOWNLOAD_STATUS_UNKNOWN,
)
from qdarchive_seeding.core.entities import DatasetRecord, FailureRecord, RunInfo
from qdarchive_seeding.core.interfaces import Checkpoint, ResumableSink
from qdarchive_seeding.core.interfaces import ProgressBus as ProgressBusProto

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


@dataclass(slots=True)
class DownloadDecision:
    """Controls what fraction of collected datasets to download."""

    download_all: bool = True
    percentage: int = 100  # 1-100
    exact_count: int | None = None  # if set, download exactly this many datasets


@dataclass(slots=True)
class DownloadCounters:
    """Mutable download statistics shared across concurrent download tasks."""

    downloaded: int = 0
    failed: int = 0
    skipped: int = 0
    access_denied: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    failures: list[FailureRecord] = field(default_factory=list)


@dataclass(slots=True)
class _DownloadCtx:
    """Bundle of state shared by download methods during a pipeline run."""

    ctx: ConcreteRunContext
    counters: DownloadCounters
    container: Container
    bus: Any  # ProgressBus
    log: logging.Logger
    skip_icpsr: bool
    icpsr_terms_url_callback: Callable[..., Any] | None
    icpsr_prompt_lock: asyncio.Lock
    download_sem: asyncio.Semaphore
    downloads_root: Path
    # Values for progress publishing (extracted/transformed are read-only here)
    extracted: int
    transformed: int
    download_asset_count: int  # mutated by _process_dataset
    pre_excluded: int


def _get_icpsr_browser_cookies(
    config: PipelineConfig,
    cache: dict[str, dict[str, str]],
    domain: str = "www.icpsr.umich.edu",
) -> dict[str, str]:
    """Extract and cache browser cookies for an ICPSR domain."""
    if domain in cache:
        return cache[domain]

    ext_auth = config.external_auth.get(domain)
    if ext_auth is None or ext_auth.type != "browser_session":
        cache[domain] = {}
        return cache[domain]

    # Cookie domain for browser lookup (leading dot matches subdomains)
    cookie_domain = "." + domain.removeprefix("www.")

    try:
        import browser_cookie3  # type: ignore[import-untyped]

        loader = {"chromium": browser_cookie3.chromium, "chrome": browser_cookie3.chrome}.get(
            ext_auth.browser, browser_cookie3.chromium
        )
        cookie_jar = loader(domain_name=cookie_domain)
        cache[domain] = {c.name: c.value for c in cookie_jar if c.value}
    except Exception:
        cache[domain] = {}
        logger.warning("Failed to extract browser cookies for %s", domain)

    return cache[domain]


def _validate_zip_members(zf: zipfile.ZipFile, target_dir: Path) -> None:
    """Reject ZIPs containing path-traversal entries or oversized files."""
    resolved_target = target_dir.resolve()
    for member in zf.infolist():
        member_path = (target_dir / member.filename).resolve()
        if (
            not str(member_path).startswith(str(resolved_target) + os.sep)
            and member_path != resolved_target
        ):
            raise ValueError(f"Path traversal detected: {member.filename}")
        if member.file_size > 500 * 1024 * 1024:  # 500 MB per file
            raise ValueError(f"File too large: {member.filename} ({member.file_size} bytes)")


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
            _validate_zip_members(zf, target_dir)
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
    _ACCESS_CODES = (
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
    _TRANSIENT_CODES = (
        "name resolution",
        "timed out",
        "ConnectError",
        "RemoteProtocolError",
        "Server disconnected",
    )

    def __init__(self, container: Container) -> None:
        self._c = container
        self._icpsr_cookie_cache: dict[str, dict[str, str]] = {}

    async def _download_classic_icpsr(
        self,
        asset: Any,
        target_dir: Path,
        config: PipelineConfig,
        terms_callback: Callable[..., Any] | None,
        prompt_lock: asyncio.Lock,
    ) -> Path | None:
        """Orchestrate Classic ICPSR download with cookie refresh and terms prompt."""
        from qdarchive_seeding.infra.storage.icpsr_downloader import download_classic_icpsr

        cookies = _get_icpsr_browser_cookies(config, self._icpsr_cookie_cache)
        zip_path = await asyncio.to_thread(download_classic_icpsr, asset, target_dir, cookies)
        if zip_path is None and terms_callback is not None:
            async with prompt_lock:
                await asyncio.to_thread(terms_callback, asset.asset_url)
                self._icpsr_cookie_cache.pop("www.icpsr.umich.edu", None)
                cookies = _get_icpsr_browser_cookies(config, self._icpsr_cookie_cache)
                zip_path = await asyncio.to_thread(
                    download_classic_icpsr, asset, target_dir, cookies
                )
        return zip_path

    async def _download_open_icpsr(
        self,
        asset: Any,
        target_dir: Path,
        config: PipelineConfig,
        terms_callback: Callable[..., Any] | None,
        prompt_lock: asyncio.Lock,
    ) -> Path | None:
        """Orchestrate Open ICPSR download with dual-cookie refresh and terms prompt."""
        from qdarchive_seeding.infra.storage.icpsr_downloader import download_open_icpsr

        open_cookies = _get_icpsr_browser_cookies(
            config, self._icpsr_cookie_cache, domain="www.openicpsr.org"
        )
        classic_cookies = _get_icpsr_browser_cookies(config, self._icpsr_cookie_cache)
        zip_path = await asyncio.to_thread(
            download_open_icpsr, asset, target_dir, open_cookies, classic_cookies
        )
        if zip_path is None and terms_callback is not None:
            async with prompt_lock:
                await asyncio.to_thread(terms_callback, asset.asset_url)
                self._icpsr_cookie_cache.pop("www.openicpsr.org", None)
                self._icpsr_cookie_cache.pop("www.icpsr.umich.edu", None)
                open_cookies = _get_icpsr_browser_cookies(
                    config, self._icpsr_cookie_cache, domain="www.openicpsr.org"
                )
                classic_cookies = _get_icpsr_browser_cookies(config, self._icpsr_cookie_cache)
                zip_path = await asyncio.to_thread(
                    download_open_icpsr, asset, target_dir, open_cookies, classic_cookies
                )
        return zip_path

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

        # ===== Phase 2: Download =====
        if "download" in phases and not metadata_only and not dry_run:
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
                counters.skipped = total_assets
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

                dctx = _DownloadCtx(
                    ctx=ctx,
                    counters=counters,
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
                    download_asset_count=download_asset_count,
                    pre_excluded=pre_excluded,
                )

                # Process datasets with limited concurrency
                dataset_sem = asyncio.Semaphore(5)

                async def _throttled_dataset(record: Any, dataset_id: str) -> None:
                    async with dataset_sem:
                        await self._process_dataset(dctx, record, dataset_id)

                dataset_tasks = [
                    _throttled_dataset(record, dataset_id)
                    for record, dataset_id in zip(
                        records_to_download, dataset_ids_to_download, strict=True
                    )
                ]
                await asyncio.gather(*dataset_tasks)
                if counters.access_denied > 0:
                    log.info(
                        "Suppressed %d download errors (access denied / network)",
                        counters.access_denied,
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
    # Extracted download helpers (were closures inside run())
    # ------------------------------------------------------------------

    async def _publish_download_progress(self, dctx: _DownloadCtx) -> None:
        """Publish current counter state to the progress bus."""
        ct = dctx.counters
        dctx.bus.publish(
            CountersUpdated(
                extracted=dctx.extracted,
                transformed=dctx.transformed,
                downloaded=ct.downloaded,
                failed=ct.failed,
                skipped=ct.skipped - dctx.pre_excluded,
                access_denied=ct.access_denied,
                total_assets=dctx.download_asset_count,
            )
        )

    async def _download_one(
        self,
        dctx: _DownloadCtx,
        asset_ref: Any,
        target: Path,
    ) -> tuple[Any, Any | None, Exception | None]:
        """Download a single asset, updating counters in *dctx*."""
        ct = dctx.counters
        c = dctx.container
        bus = dctx.bus
        log = dctx.log

        async with dctx.download_sem:
            if dctx.ctx.cancelled:
                return asset_ref, None, None
            if c.policy.should_skip_asset(asset_ref):
                if asset_ref.download_status != DOWNLOAD_STATUS_SUCCESS:
                    asset_ref.download_status = DOWNLOAD_STATUS_SKIPPED
                async with ct.lock:
                    ct.skipped += 1
                    await self._publish_download_progress(dctx)
                return asset_ref, None, None
            # Skip ICPSR assets if user declined
            if dctx.skip_icpsr and (
                "openicpsr.org" in asset_ref.asset_url or "icpsr.umich.edu" in asset_ref.asset_url
            ):
                asset_ref.download_status = DOWNLOAD_STATUS_SKIPPED
                async with ct.lock:
                    ct.skipped += 1
                    await self._publish_download_progress(dctx)
                return asset_ref, None, None
            # Delay between downloads to avoid overwhelming the server
            await asyncio.sleep(1.0)

            # Notify UI that this file download is starting
            _start_filename = (
                asset_ref.local_filename
                or asset_ref.asset_url.rsplit("/", 1)[-1].split("?")[0]
                or "file"
            )
            bus.publish(
                AssetDownloadStarted(
                    asset_url=asset_ref.asset_url,
                    filename=_start_filename,
                )
            )

            # ICPSR downloads (Classic zipcart2 or Open ICPSR)
            _is_icpsr = "zipcart2" in asset_ref.asset_url or "openicpsr.org" in asset_ref.asset_url
            if _is_icpsr:
                try:
                    if "zipcart2" in asset_ref.asset_url:
                        zip_path = await self._download_classic_icpsr(
                            asset_ref,
                            target,
                            c.config,
                            dctx.icpsr_terms_url_callback,
                            dctx.icpsr_prompt_lock,
                        )
                        fail_msg = "ICPSR form flow failed"
                    else:
                        zip_path = await self._download_open_icpsr(
                            asset_ref,
                            target,
                            c.config,
                            dctx.icpsr_terms_url_callback,
                            dctx.icpsr_prompt_lock,
                        )
                        fail_msg = "Open ICPSR download failed"
                    if zip_path is not None:
                        asset_ref.download_status = DOWNLOAD_STATUS_SUCCESS
                        asset_ref.asset_type = "zip_bundle"
                        asset_ref.downloaded_at = datetime.now(UTC)
                        async with ct.lock:
                            ct.downloaded += 1
                            await self._publish_download_progress(dctx)
                        bus.publish(
                            AssetDownloadUpdate(
                                asset_url=asset_ref.asset_url,
                                status=DOWNLOAD_STATUS_SUCCESS,
                                bytes_downloaded=asset_ref.size_bytes or 0,
                            )
                        )
                    else:
                        asset_ref.download_status = DOWNLOAD_STATUS_FAILED
                        asset_ref.error_message = fail_msg
                        async with ct.lock:
                            ct.failed += 1
                            await self._publish_download_progress(dctx)
                        bus.publish(
                            AssetDownloadUpdate(
                                asset_url=asset_ref.asset_url,
                                status=DOWNLOAD_STATUS_FAILED,
                                error_message=fail_msg,
                            )
                        )
                except Exception as icpsr_exc:
                    asset_ref.download_status = DOWNLOAD_STATUS_FAILED
                    asset_ref.error_message = str(icpsr_exc)
                    async with ct.lock:
                        ct.failed += 1
                        await self._publish_download_progress(dctx)
                    bus.publish(
                        AssetDownloadUpdate(
                            asset_url=asset_ref.asset_url,
                            status=DOWNLOAD_STATUS_FAILED,
                            error_message=str(icpsr_exc),
                        )
                    )
                return asset_ref, None, None

            # Per-asset progress callback
            _asset_filename = (
                asset_ref.local_filename
                or asset_ref.asset_url.rsplit("/", 1)[-1].split("?")[0]
                or "file"
            )

            def _on_progress(bytes_so_far: int, total_bytes: int | None) -> None:
                bus.publish(
                    AssetDownloadProgress(
                        asset_url=asset_ref.asset_url,
                        bytes_downloaded=bytes_so_far,
                        total_bytes=total_bytes,
                        filename=_asset_filename,
                    )
                )

            # Retry loop for transient errors (DNS, timeout, connection)
            max_retries = 3
            last_exc: Exception | None = None
            for attempt in range(max_retries + 1):
                try:
                    dl_result = await c.downloader.download(
                        asset_ref, target, progress_callback=_on_progress
                    )
                    # Success — update counters
                    async with ct.lock:
                        if dl_result and dl_result.asset.download_status == DOWNLOAD_STATUS_SUCCESS:
                            ct.downloaded += 1
                            bus.publish(
                                AssetDownloadUpdate(
                                    asset_url=asset_ref.asset_url,
                                    status=DOWNLOAD_STATUS_SUCCESS,
                                    bytes_downloaded=dl_result.bytes_downloaded,
                                )
                            )
                        else:
                            ct.failed += 1
                            ct.failures.append(
                                FailureRecord(
                                    asset_url=asset_ref.asset_url,
                                    error="non-success status",
                                )
                            )
                            bus.publish(
                                AssetDownloadUpdate(
                                    asset_url=asset_ref.asset_url,
                                    status=DOWNLOAD_STATUS_FAILED,
                                    error_message="non-success status",
                                )
                            )
                        await self._publish_download_progress(dctx)
                    return asset_ref, dl_result, None
                except Exception as dl_exc:
                    last_exc = dl_exc
                    err_str = str(dl_exc)
                    is_transient = any(code in err_str for code in self._TRANSIENT_CODES)
                    if is_transient and attempt < max_retries:
                        backoff = 2 ** (attempt + 1)
                        log.debug(
                            "Transient error for %s (attempt %d/%d), retrying in %ds: %s",
                            asset_ref.asset_url,
                            attempt + 1,
                            max_retries + 1,
                            backoff,
                            err_str,
                        )
                        await asyncio.sleep(backoff)
                        continue
                    # Permanent error or retries exhausted
                    break

            # All retries failed
            assert last_exc is not None  # noqa: S101
            err_str = str(last_exc)
            is_permanent = any(code in err_str for code in self._ACCESS_CODES)
            # Permanent access errors (403, 401, etc.) → SKIPPED so
            # they are never retried; transient/unknown → FAILED.
            final_status = DOWNLOAD_STATUS_SKIPPED if is_permanent else DOWNLOAD_STATUS_FAILED
            asset_ref.download_status = final_status
            asset_ref.error_message = err_str
            is_suppressed = (
                not err_str.strip()
                or is_permanent
                or any(code in err_str for code in self._TRANSIENT_CODES)
            )
            async with ct.lock:
                ct.failed += 1
                ct.failures.append(FailureRecord(asset_url=asset_ref.asset_url, error=err_str))
                if is_suppressed:
                    ct.access_denied += 1
                else:
                    bus.publish(
                        ErrorEvent(
                            component="downloader",
                            error_type=type(last_exc).__name__,
                            message=err_str,
                            asset_url=asset_ref.asset_url,
                        )
                    )
                    log.error(
                        "Download failed for %s: %s",
                        asset_ref.asset_url,
                        last_exc,
                    )
                    await self._publish_download_progress(dctx)
                bus.publish(
                    AssetDownloadUpdate(
                        asset_url=asset_ref.asset_url,
                        status=final_status,
                        error_message=err_str,
                    )
                )
                return asset_ref, None, last_exc

    async def _process_dataset(
        self,
        dctx: _DownloadCtx,
        record: Any,
        dataset_id: str,
    ) -> None:
        """Process a single dataset: download its assets, extract ZIPs, update sink."""
        ct = dctx.counters
        c = dctx.container
        log = dctx.log

        if dctx.ctx.cancelled:
            return

        # Skip datasets rejected by the policy (e.g. size threshold).
        if c.policy.should_skip_dataset(record):
            log.info(
                "Skipping dataset %s: rejected by policy",
                record.source_dataset_id,
            )
            async with ct.lock:
                ct.skipped += len(record.assets)
                await self._publish_download_progress(dctx)
            return

        # Restore prior download statuses from DB so the policy
        # can skip already-downloaded files on resume.
        if isinstance(c.sink, ResumableSink):
            prior_statuses = c.sink.get_file_statuses(dataset_id)
            for asset in record.assets:
                fname = asset.local_filename or asset.asset_url.rsplit("/", 1)[-1]
                status = prior_statuses.get(asset.asset_url) or prior_statuses.get(fname)
                if status and status != "UNKNOWN":
                    asset.download_status = status

        dataset_slug = (
            record.download_project_folder
            or (record.raw.get("dataset_slug") if record.raw else None)
            or "dataset"
        )
        target_dir = c.path_strategy.dataset_dir(
            dctx.downloads_root,
            source_name=record.source_name,
            dataset_slug=dataset_slug,
        )
        c.filesystem.ensure_dir(target_dir)

        tasks = [self._download_one(dctx, asset, target_dir) for asset in record.assets]
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
                    async with ct.lock:
                        extra = len(zip_files) - 1
                        ct.downloaded += extra
                        dctx.download_asset_count += extra
                        await self._publish_download_progress(dctx)

        # Update sink with download results (skip policy-skipped assets
        # to avoid overwriting good DB statuses like SUCCESS with SKIPPED)
        try:
            for asset in record.assets:
                if asset.download_status != DOWNLOAD_STATUS_SKIPPED:
                    c.sink.upsert_asset(dataset_id, asset)
        except Exception as exc:
            log.error("Sink update error for %s: %s", dataset_id, exc)
