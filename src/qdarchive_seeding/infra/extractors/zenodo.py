from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import PurePosixPath
from typing import Any

from qdarchive_seeding.core.constants import (
    DOWNLOAD_METHOD_API,
    PERSON_ROLE_CONTRIBUTOR,
    PERSON_ROLE_CREATOR,
)
from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord, PersonRole
from qdarchive_seeding.core.interfaces import AuthProvider, HttpClient, RunContext
from qdarchive_seeding.core.progress import DateSliceProgress, PageProgress, QueryProgress
from qdarchive_seeding.infra.extractors._utils import extract_year as _extract_year
from qdarchive_seeding.infra.http.auth import apply_auth_async
from qdarchive_seeding.infra.http.pagination import PagePagination

logger = logging.getLogger(__name__)


_DATE_SPLIT_THRESHOLD = 9500
_ZENODO_EPOCH = date(2013, 1, 1)  # Zenodo launched in 2013


@dataclass(slots=True)
class ZenodoOptions:
    include_files: bool = True
    max_pages: int | None = None
    ext_batch_size: int = 10
    nl_batch_size: int = 4
    auto_date_split: bool = True
    _safety_max_pages: int = 200


@dataclass(slots=True)
class ZenodoExtractor:
    http_client: HttpClient
    auth: AuthProvider
    options: ZenodoOptions

    async def extract(self, ctx: RunContext) -> AsyncIterator[DatasetRecord]:
        """Yield records, iterating over multiple queries if search_strategy is set."""
        strategy = ctx.config.source.search_strategy
        if strategy is None:
            # Legacy single-query mode
            query = ctx.config.source.params.get("q", "")
            async for record in self._extract_single_query(
                ctx, str(query), query_string=str(query)
            ):
                yield record
            return

        # Pre-populate seen_ids from existing records for resume support
        existing_ids = ctx.existing_dataset_ids
        seen_ids: set[str] = set(existing_ids) if existing_ids else set()
        prefix = strategy.base_query_prefix
        facets = strategy.facet_filters
        bus = ctx.progress_bus

        # Batch both extension and NL queries to avoid Zenodo API timeouts
        ext_batches = _batched(strategy.extension_queries, self.options.ext_batch_size)
        nl_batches = _batched(strategy.natural_language_queries, self.options.nl_batch_size)
        total_queries = len(ext_batches) + len(nl_batches)
        query_idx = 0

        # Extension-based queries: batched OR queries with prefix
        for batch_idx, batch in enumerate(ext_batches):
            query_idx += 1
            ext_clauses = " OR ".join(f"filetype:{ext}" for ext in batch)
            query = f"{prefix} ({ext_clauses})".strip() if prefix else f"({ext_clauses})"
            label = f"ext batch {batch_idx + 1}/{len(ext_batches)} ({len(batch)} types)"
            if bus:
                bus.publish(
                    QueryProgress(
                        current_query=query_idx,
                        total_queries=total_queries,
                        query_label=label,
                        query_type="extension",
                    )
                )
            async for record in self._extract_with_date_splitting(
                ctx, query, seen_ids=seen_ids, query_string=label, extra_params=facets
            ):
                yield record

        # NL queries: combine into batched OR queries to avoid Zenodo timeouts.
        for batch_idx, batch in enumerate(nl_batches):
            query_idx += 1
            nl_clauses = " OR ".join(f"({nlq})" for nlq in batch)
            query = f"({nl_clauses})"
            label = f"nl batch {batch_idx + 1}/{len(nl_batches)} ({len(batch)} terms)"
            if bus:
                bus.publish(
                    QueryProgress(
                        current_query=query_idx,
                        total_queries=total_queries,
                        query_label=label,
                        query_type="nl",
                    )
                )
            async for record in self._extract_with_date_splitting(
                ctx, query, seen_ids=seen_ids, query_string=label, extra_params=facets
            ):
                yield record

    async def _extract_with_date_splitting(
        self,
        ctx: RunContext,
        query: str,
        *,
        seen_ids: set[str] | None = None,
        query_string: str = "",
        extra_params: dict[str, str] | None = None,
    ) -> AsyncIterator[DatasetRecord]:
        """Extract records, splitting by date range if results exceed 10k."""
        if not self.options.auto_date_split:
            async for record in self._extract_single_query(
                ctx,
                query,
                seen_ids=seen_ids,
                query_string=query_string,
                extra_params=extra_params,
            ):
                yield record
            return

        endpoint = ctx.config.source.endpoints.get("search", "/records")
        base_url = ctx.config.source.base_url.rstrip("/")
        url = f"{base_url}{endpoint}"

        # Build params for probing (same as _extract_single_query)
        probe_params: dict[str, Any] = dict(ctx.config.source.params)
        probe_params["q"] = query
        if extra_params:
            probe_params.update(extra_params)

        total = await _probe_total(self.http_client, self.auth, url, probe_params)
        logger.info(
            "Query '%s' has %d total results",
            query_string,
            total,
        )

        if total <= _DATE_SPLIT_THRESHOLD:
            async for record in self._extract_single_query(
                ctx,
                query,
                seen_ids=seen_ids,
                query_string=query_string,
                extra_params=extra_params,
            ):
                yield record
            return

        # Need date splitting — use cached slices if available
        checkpoint = ctx.checkpoint
        cached = checkpoint.get_date_slices(query_string) if checkpoint else None
        if cached is not None:
            slices = [(date.fromisoformat(s), date.fromisoformat(e)) for s, e in cached]
            logger.info(
                "Query '%s' using %d cached date slices",
                query_string,
                len(slices),
            )
        else:
            today = date.today()
            slices = await _find_date_slices(
                self.http_client,
                self.auth,
                url,
                probe_params,
                _ZENODO_EPOCH,
                today,
            )
            logger.info(
                "Query '%s' split into %d date slices to stay under %d results each",
                query_string,
                len(slices),
                _DATE_SPLIT_THRESHOLD,
            )
            if checkpoint and slices:
                checkpoint.set_date_slices(
                    query_string,
                    [(s.isoformat(), e.isoformat()) for s, e in slices],
                )

        bus = ctx.progress_bus
        for slice_idx, (start, end) in enumerate(slices):
            date_query = f"{query} AND created:[{start} TO {end}]"
            slice_label = f"[{start}→{end}]"
            logger.debug(
                "Date slice %d/%d: %s %s",
                slice_idx + 1,
                len(slices),
                query_string,
                slice_label,
            )
            if bus:
                bus.publish(
                    DateSliceProgress(
                        current_slice=slice_idx + 1,
                        total_slices=len(slices),
                        query_label=query_string,
                        slice_label=slice_label,
                    )
                )
            async for record in self._extract_single_query(
                ctx,
                date_query,
                seen_ids=seen_ids,
                query_string=f"{query_string} {slice_label}",
                extra_params=extra_params,
            ):
                yield record

    async def _extract_single_query(
        self,
        ctx: RunContext,
        query: str,
        *,
        seen_ids: set[str] | None = None,
        query_string: str = "",
        extra_params: dict[str, str] | None = None,
    ) -> AsyncIterator[DatasetRecord]:
        """Run a single paginated query against the Zenodo API.

        # TODO(H5): extract shared pagination/checkpoint logic into
        # infra/extractors/_pagination.py — the checkpoint resume, failed-page
        # retry, and completion-marking patterns are duplicated across Zenodo,
        # Harvard Dataverse, and Syracuse QDR extractors.
        """
        endpoint = ctx.config.source.endpoints.get("search", "/records")
        base_url = ctx.config.source.base_url.rstrip("/")
        url = f"{base_url}{endpoint}"

        headers: dict[str, str] = {}
        params = dict(ctx.config.source.params)
        params["q"] = query
        if extra_params:
            params.update(extra_params)
        headers, params = await apply_auth_async(self.auth, headers, params)

        pagination = ctx.config.source.pagination
        paginator = PagePagination(
            page_param=(pagination.page_param if pagination and pagination.page_param else "page"),
            size_param=(pagination.size_param if pagination and pagination.size_param else "size"),
        )

        source_cfg = ctx.config.source
        effective_max_pages = self.options.max_pages or self.options._safety_max_pages
        total_yielded = 0
        bus = ctx.progress_bus
        checkpoint = ctx.checkpoint

        # Resume support: skip if this query was already completed
        if checkpoint is not None and checkpoint.is_query_complete(query_string):
            logger.info("Skipping completed query '%s' (checkpoint)", query_string)
            return

        # --- Retry previously failed pages before continuing ---
        if checkpoint is not None:
            failed_pages = checkpoint.get_failed_pages(query_string)
            if failed_pages:
                logger.info(
                    "Retrying %d failed pages for query '%s'",
                    len(failed_pages),
                    query_string,
                )
                page_param = (
                    pagination.page_param if pagination and pagination.page_param else "page"
                )
                for failed_page in list(failed_pages):
                    retry_params = {**params, page_param: failed_page + 1}  # 1-indexed
                    try:
                        response = await self.http_client.get(
                            url,
                            headers=headers,
                            params=retry_params,
                        )
                    except Exception as exc:
                        logger.error(
                            "Retry still failing for query '%s' page %d: %s",
                            query_string,
                            failed_page + 1,
                            exc,
                        )
                        continue  # Leave in failed_pages for next run

                    payload = response.json()
                    hits = payload.get("hits", {}).get("hits", [])
                    checkpoint.clear_failed_page(query_string, failed_page)
                    if not hits:
                        continue
                    total_yielded += len(hits)
                    logger.info(
                        "Retry succeeded for query '%s' page %d: %d hits",
                        query_string,
                        failed_page + 1,
                        len(hits),
                    )
                    for item in hits:
                        record = self._build_record(
                            item,
                            source_cfg,
                            url,
                            query_string,
                            seen_ids,
                        )
                        if record is not None:
                            yield record

        # Resume support: skip already-fetched pages
        resume_from = checkpoint.get_start_page(query_string) if checkpoint else 0

        for page_count, page_params in enumerate(paginator.iter_params(params)):
            if page_count >= effective_max_pages:
                logger.warning("Reached max pages limit (%d), stopping", effective_max_pages)
                break
            if page_count < resume_from:
                continue  # Skip pages already checkpointed
            try:
                response = await self.http_client.get(url, headers=headers, params=page_params)
            except Exception as exc:
                logger.error(
                    "HTTP request failed for query '%s' page %d: %s, skipping page",
                    query_string,
                    page_count + 1,
                    exc,
                )
                if checkpoint is not None:
                    checkpoint.mark_page_failed(query_string, page_count)
                continue
            payload = response.json()
            hits_data = payload.get("hits", {})
            hits = hits_data.get("hits", [])
            api_total = hits_data.get("total", 0)
            if not isinstance(hits, list):
                break
            if not hits:
                break
            total_yielded += len(hits)
            if bus:
                bus.publish(
                    PageProgress(
                        current_page=page_count + 1,
                        total_hits=api_total,
                        query_label=query_string,
                    )
                )
            # Checkpoint after successful page fetch
            if checkpoint is not None:
                checkpoint.mark_page(query_string, page_count + 1, len(hits))

            logger.debug(
                "Query '%s' page %d: fetched %d hits (%d total)",
                query_string,
                page_count + 1,
                len(hits),
                total_yielded,
            )
            for item in hits:
                record = self._build_record(
                    item,
                    source_cfg,
                    url,
                    query_string,
                    seen_ids,
                )
                if record is not None:
                    yield record

        # Only mark complete if no failed pages remain
        if checkpoint is not None:
            if not checkpoint.get_failed_pages(query_string):
                checkpoint.mark_query_complete(query_string)
            else:
                logger.warning(
                    "Query '%s' has %d failed pages, not marking complete",
                    query_string,
                    len(checkpoint.get_failed_pages(query_string)),
                )

    def _build_record(
        self,
        item: dict[str, Any],
        source_cfg: Any,
        fallback_url: str,
        query_string: str,
        seen_ids: set[str] | None,
    ) -> DatasetRecord | None:
        """Build a DatasetRecord from a Zenodo API hit. Returns None if duplicate."""
        record_id = str(item.get("id")) if item.get("id") is not None else None

        if seen_ids is not None and record_id:
            if record_id in seen_ids:
                return None
            seen_ids.add(record_id)

        metadata = item.get("metadata", {})
        files = item.get("files", []) if self.options.include_files else []
        # Capture auth headers so the downloader can apply them per-request
        auth_headers, _ = self.auth.apply({}, {})
        asset_meta = {"auth_headers": auth_headers} if auth_headers else None
        assets = [
            AssetRecord(
                asset_url=f.get("links", {}).get("self", ""),
                local_filename=f.get("key"),
                file_type=PurePosixPath(f.get("key", "")).suffix.lstrip(".")
                if f.get("key")
                else None,
                size_bytes=f.get("size"),
                metadata=asset_meta,
            )
            for f in files
            if f
        ]

        persons: list[PersonRole] = []
        for creator in metadata.get("creators", []):
            if creator.get("name"):
                persons.append(PersonRole(name=creator["name"], role=PERSON_ROLE_CREATOR))
        for contributor in metadata.get("contributors", []):
            if contributor.get("name"):
                persons.append(PersonRole(name=contributor["name"], role=PERSON_ROLE_CONTRIBUTOR))

        return DatasetRecord(
            source_name=source_cfg.name,
            source_dataset_id=record_id,
            source_url=item.get("links", {}).get("self", fallback_url),
            title=metadata.get("title"),
            description=metadata.get("description"),
            doi=metadata.get("doi"),
            license=_extract_license(metadata.get("license")),
            query_string=query_string,
            repository_id=source_cfg.repository_id,
            repository_url=source_cfg.repository_url,
            version=metadata.get("version"),
            language=metadata.get("language"),
            upload_date=metadata.get("publication_date"),
            download_method=DOWNLOAD_METHOD_API,
            download_repository_folder=source_cfg.name,
            download_project_folder=record_id,
            download_version_folder=metadata.get("version"),
            keywords=metadata.get("keywords", []),
            persons=persons,
            year=_extract_year(metadata.get("publication_date")),
            owner_name=metadata.get("creators", [{}])[0].get("name")
            if metadata.get("creators")
            else None,
            assets=assets,
            raw=item,
        )


def _batched(items: list[str], size: int) -> list[list[str]]:
    """Split *items* into sub-lists of at most *size* elements.

    # TODO: replace with itertools.batched when min Python is 3.12
    """
    if not items:
        return []
    return [items[i : i + size] for i in range(0, len(items), size)]


async def _probe_total(
    http_client: HttpClient,
    auth: AuthProvider,
    url: str,
    base_params: dict[str, Any],
) -> int:
    """Probe the total hit count for a query using size=1 (cheapest request)."""
    headers: dict[str, str] = {}
    params = {**base_params, "size": 1, "page": 1}
    headers, params = await apply_auth_async(auth, headers, params)
    try:
        resp = await http_client.get(url, headers=headers, params=params)
        total: int = resp.json().get("hits", {}).get("total", 0)
        return total
    except Exception as exc:
        logger.warning("Probe request failed: %s", exc)
        return 0


async def _find_date_slices(
    http_client: HttpClient,
    auth: AuthProvider,
    url: str,
    base_params: dict[str, Any],
    start: date,
    end: date,
    threshold: int = _DATE_SPLIT_THRESHOLD,
) -> list[tuple[date, date]]:
    """Recursively split a date range until each slice has ≤ threshold results."""
    params = {**base_params}
    base_q = params.get("q", "")
    params["q"] = f"{base_q} AND created:[{start} TO {end}]"

    total = await _probe_total(http_client, auth, url, params)

    if total == 0:
        return []
    if total <= threshold:
        return [(start, end)]
    if start >= end:
        # Single day still over threshold — can't split further, accept the cap
        logger.warning(
            "Single day %s has %d results (> %d), accepting 10k cap",
            start,
            total,
            threshold,
        )
        return [(start, end)]

    # Split in half — probe both halves concurrently
    mid = start + (end - start) // 2
    logger.debug(
        "Splitting [%s, %s] (%d results) into [%s, %s] + [%s, %s]",
        start,
        end,
        total,
        start,
        mid,
        mid + timedelta(days=1),
        end,
    )
    left, right = await asyncio.gather(
        _find_date_slices(http_client, auth, url, base_params, start, mid, threshold),
        _find_date_slices(
            http_client, auth, url, base_params, mid + timedelta(days=1), end, threshold
        ),
    )
    return left + right


def _extract_license(value: object) -> str | None:
    """Zenodo returns license as a dict like {"id": "cc-by-4.0"} or a string."""
    if value is None:
        return None
    if isinstance(value, dict):
        return str(value.get("id", value.get("title", "")))
    return str(value)
