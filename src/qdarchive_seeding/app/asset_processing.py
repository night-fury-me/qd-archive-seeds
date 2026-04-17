"""Download-phase asset processing: per-asset download, ICPSR dispatch, error classification."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from qdarchive_seeding.app.download_strategy import DownloadCtx, get_icpsr_browser_cookies
from qdarchive_seeding.core.constants import (
    DOWNLOAD_STATUS_FAILED,
    DOWNLOAD_STATUS_FAILED_LOGIN_REQUIRED,
    DOWNLOAD_STATUS_FAILED_TOO_LARGE,
    DOWNLOAD_STATUS_SKIPPED,
    DOWNLOAD_STATUS_SUCCESS,
)
from qdarchive_seeding.core.entities import FailureRecord
from qdarchive_seeding.core.interfaces import ResumableSink
from qdarchive_seeding.core.progress import (
    AssetDownloadProgress,
    AssetDownloadStarted,
    AssetDownloadUpdate,
    CountersUpdated,
    ErrorEvent,
)
from qdarchive_seeding.infra.storage.zip_extractor import extract_zip_bundle

logger = logging.getLogger(__name__)

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


async def publish_download_progress(dctx: DownloadCtx) -> None:
    """Publish current counter state to the progress bus."""
    ct = dctx.state.counters
    dctx.cfg.bus.publish(
        CountersUpdated(
            extracted=dctx.cfg.extracted,
            transformed=dctx.cfg.transformed,
            downloaded=ct.downloaded,
            failed=ct.failed,
            skipped=ct.skipped - dctx.cfg.pre_excluded,
            access_denied=ct.access_denied,
            total_assets=dctx.state.download_asset_count,
        )
    )


async def download_classic_icpsr(
    asset: Any,
    target_dir: Path,
    config: Any,
    cookie_cache: dict[str, dict[str, str]],
    terms_callback: Callable[..., Any] | None,
    prompt_lock: asyncio.Lock,
) -> Path | None:
    """Orchestrate Classic ICPSR download with cookie refresh and terms prompt."""
    from qdarchive_seeding.infra.storage.icpsr_downloader import (
        download_classic_icpsr as _do_classic,
    )

    cookies = get_icpsr_browser_cookies(config, cookie_cache)
    zip_path = await asyncio.to_thread(_do_classic, asset, target_dir, cookies)
    if zip_path is None and terms_callback is not None:
        async with prompt_lock:
            await asyncio.to_thread(terms_callback, asset.asset_url)
            cookie_cache.pop("www.icpsr.umich.edu", None)
            cookies = get_icpsr_browser_cookies(config, cookie_cache)
            zip_path = await asyncio.to_thread(_do_classic, asset, target_dir, cookies)
    return zip_path


async def download_open_icpsr(
    asset: Any,
    target_dir: Path,
    config: Any,
    cookie_cache: dict[str, dict[str, str]],
    terms_callback: Callable[..., Any] | None,
    prompt_lock: asyncio.Lock,
) -> Path | None:
    """Orchestrate Open ICPSR download with dual-cookie refresh and terms prompt."""
    from qdarchive_seeding.infra.storage.icpsr_downloader import (
        download_open_icpsr as _do_open,
    )

    open_cookies = get_icpsr_browser_cookies(config, cookie_cache, domain="www.openicpsr.org")
    classic_cookies = get_icpsr_browser_cookies(config, cookie_cache)
    zip_path = await asyncio.to_thread(_do_open, asset, target_dir, open_cookies, classic_cookies)
    if zip_path is None and terms_callback is not None:
        async with prompt_lock:
            await asyncio.to_thread(terms_callback, asset.asset_url)
            cookie_cache.pop("www.openicpsr.org", None)
            cookie_cache.pop("www.icpsr.umich.edu", None)
            open_cookies = get_icpsr_browser_cookies(
                config, cookie_cache, domain="www.openicpsr.org"
            )
            classic_cookies = get_icpsr_browser_cookies(config, cookie_cache)
            zip_path = await asyncio.to_thread(
                _do_open, asset, target_dir, open_cookies, classic_cookies
            )
    return zip_path


async def download_one(
    dctx: DownloadCtx,
    asset_ref: Any,
    target: Path,
    cookie_cache: dict[str, dict[str, str]],
) -> tuple[Any, Any | None, Exception | None]:
    """Download a single asset, dispatching to ICPSR or generic handler."""
    ct = dctx.state.counters
    c = dctx.cfg.container
    bus = dctx.cfg.bus

    async with dctx.cfg.download_sem:
        if dctx.cfg.ctx.cancelled:
            return asset_ref, None, None
        if c.policy.should_skip_asset(asset_ref):
            if asset_ref.download_status != DOWNLOAD_STATUS_SUCCESS:
                asset_ref.download_status = DOWNLOAD_STATUS_SKIPPED
            async with ct.lock:
                ct.skipped += 1
                await publish_download_progress(dctx)
            return asset_ref, None, None
        # Skip ICPSR assets if user declined
        if dctx.cfg.skip_icpsr and (
            "openicpsr.org" in asset_ref.asset_url or "icpsr.umich.edu" in asset_ref.asset_url
        ):
            asset_ref.download_status = DOWNLOAD_STATUS_SKIPPED
            async with ct.lock:
                ct.skipped += 1
                await publish_download_progress(dctx)
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

        # Dispatch to ICPSR or generic handler
        _is_icpsr = "zipcart2" in asset_ref.asset_url or "openicpsr.org" in asset_ref.asset_url
        if _is_icpsr:
            return await _download_icpsr_asset(dctx, asset_ref, target, cookie_cache)
        return await _download_generic_asset(dctx, asset_ref, target)


async def _download_icpsr_asset(
    dctx: DownloadCtx,
    asset_ref: Any,
    target: Path,
    cookie_cache: dict[str, dict[str, str]],
) -> tuple[Any, Any | None, Exception | None]:
    """Handle ICPSR-specific download (Classic zipcart2 or Open ICPSR)."""
    ct = dctx.state.counters
    bus = dctx.cfg.bus

    try:
        if "zipcart2" in asset_ref.asset_url:
            zip_path = await download_classic_icpsr(
                asset_ref,
                target,
                dctx.cfg.container.config,
                cookie_cache,
                dctx.cfg.icpsr_terms_url_callback,
                dctx.cfg.icpsr_prompt_lock,
            )
            fail_msg = "ICPSR form flow failed"
        else:
            zip_path = await download_open_icpsr(
                asset_ref,
                target,
                dctx.cfg.container.config,
                cookie_cache,
                dctx.cfg.icpsr_terms_url_callback,
                dctx.cfg.icpsr_prompt_lock,
            )
            fail_msg = "Open ICPSR download failed"
        if zip_path is not None:
            asset_ref.download_status = DOWNLOAD_STATUS_SUCCESS
            asset_ref.asset_type = "zip_bundle"
            asset_ref.downloaded_at = datetime.now(UTC)
            async with ct.lock:
                ct.downloaded += 1
                await publish_download_progress(dctx)
            bus.publish(
                AssetDownloadUpdate(
                    asset_url=asset_ref.asset_url,
                    status=DOWNLOAD_STATUS_SUCCESS,
                    bytes_downloaded=asset_ref.size_bytes or 0,
                )
            )
        else:
            # ICPSR download failures are almost always auth/cookie issues;
            # classify as login-required so the per-row reason is correct.
            asset_ref.download_status = DOWNLOAD_STATUS_FAILED_LOGIN_REQUIRED
            asset_ref.error_message = fail_msg
            async with ct.lock:
                ct.failed += 1
                await publish_download_progress(dctx)
            bus.publish(
                AssetDownloadUpdate(
                    asset_url=asset_ref.asset_url,
                    status=DOWNLOAD_STATUS_FAILED_LOGIN_REQUIRED,
                    error_message=fail_msg,
                )
            )
    except Exception as icpsr_exc:
        asset_ref.download_status = DOWNLOAD_STATUS_FAILED_LOGIN_REQUIRED
        asset_ref.error_message = str(icpsr_exc)
        async with ct.lock:
            ct.failed += 1
            await publish_download_progress(dctx)
        bus.publish(
            AssetDownloadUpdate(
                asset_url=asset_ref.asset_url,
                status=DOWNLOAD_STATUS_FAILED_LOGIN_REQUIRED,
                error_message=str(icpsr_exc),
            )
        )
    return asset_ref, None, None


async def _download_generic_asset(
    dctx: DownloadCtx,
    asset_ref: Any,
    target: Path,
) -> tuple[Any, Any | None, Exception | None]:
    """Download a standard (non-ICPSR) asset with retry logic."""
    ct = dctx.state.counters
    c = dctx.cfg.container
    bus = dctx.cfg.bus
    log = dctx.cfg.log

    # Per-asset progress callback
    _asset_filename = (
        asset_ref.local_filename or asset_ref.asset_url.rsplit("/", 1)[-1].split("?")[0] or "file"
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
                await publish_download_progress(dctx)
            return asset_ref, dl_result, None
        except Exception as dl_exc:
            last_exc = dl_exc
            err_str = str(dl_exc)
            is_transient = any(code in err_str for code in _TRANSIENT_CODES)
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

    # All retries failed — classify and record the error
    return await _classify_download_error(dctx, asset_ref, last_exc)


async def _classify_download_error(
    dctx: DownloadCtx,
    asset_ref: Any,
    last_exc: Exception | None,
) -> tuple[Any, None, Exception | None]:
    """Classify a download error as permanent or transient and update counters."""
    ct = dctx.state.counters
    bus = dctx.cfg.bus
    log = dctx.cfg.log

    assert last_exc is not None  # noqa: S101
    err_str = str(last_exc)
    is_permanent = any(code in err_str for code in _ACCESS_CODES)
    final_status = DOWNLOAD_STATUS_FAILED_LOGIN_REQUIRED if is_permanent else DOWNLOAD_STATUS_FAILED
    asset_ref.download_status = final_status
    asset_ref.error_message = err_str
    is_suppressed = (
        not err_str.strip() or is_permanent or any(code in err_str for code in _TRANSIENT_CODES)
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
            await publish_download_progress(dctx)
        bus.publish(
            AssetDownloadUpdate(
                asset_url=asset_ref.asset_url,
                status=final_status,
                error_message=err_str,
            )
        )
        return asset_ref, None, last_exc


async def process_dataset(
    dctx: DownloadCtx,
    record: Any,
    dataset_id: str,
    cookie_cache: dict[str, dict[str, str]],
) -> None:
    """Process a single dataset: download its assets, extract ZIPs, update sink."""
    ct = dctx.state.counters
    c = dctx.cfg.container
    log = dctx.cfg.log

    if dctx.cfg.ctx.cancelled:
        return

    # Skip datasets rejected by the policy (e.g. size threshold).
    if c.policy.should_skip_dataset(record):
        log.info(
            "Skipping dataset %s: rejected by policy",
            record.source_dataset_id,
        )
        for asset in record.assets:
            asset.download_status = DOWNLOAD_STATUS_FAILED_TOO_LARGE
            asset.error_message = "Skipped: dataset exceeds size threshold"
            with contextlib.suppress(Exception):
                c.sink.upsert_asset(dataset_id, asset)
        async with ct.lock:
            ct.skipped += len(record.assets)
            await publish_download_progress(dctx)
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
        dctx.cfg.downloads_root,
        source_name=record.source_name,
        dataset_slug=dataset_slug,
    )
    c.filesystem.ensure_dir(target_dir)

    tasks = [download_one(dctx, asset, target_dir, cookie_cache) for asset in record.assets]
    # Counter updates happen inside download_one for real-time progress
    await asyncio.gather(*tasks)

    # Extract ZIP bundles and update asset records
    for asset in list(record.assets):
        is_zip = asset.asset_type == "zip_bundle" or (asset.local_filename or "").lower().endswith(
            ".zip"
        )
        if (
            asset.download_status == DOWNLOAD_STATUS_SUCCESS
            and is_zip
            and asset.local_dir
            and asset.local_filename
        ):
            zip_files = extract_zip_bundle(
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
                    dctx.state.download_asset_count += extra
                    await publish_download_progress(dctx)

    # Update sink with download results (skip policy-skipped assets
    # to avoid overwriting good DB statuses like SUCCESS with SKIPPED)
    try:
        for asset in record.assets:
            if asset.download_status != DOWNLOAD_STATUS_SKIPPED:
                c.sink.upsert_asset(dataset_id, asset)
    except Exception as exc:
        log.error("Sink update error for %s: %s", dataset_id, exc)
