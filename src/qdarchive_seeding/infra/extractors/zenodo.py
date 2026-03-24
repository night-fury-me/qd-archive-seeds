from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import PurePosixPath
from typing import Any

from qdarchive_seeding.app.progress import DateSliceProgress, PageProgress, QueryProgress
from qdarchive_seeding.core.constants import (
    DOWNLOAD_METHOD_API,
    PERSON_ROLE_CONTRIBUTOR,
    PERSON_ROLE_CREATOR,
)
from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord, PersonRole
from qdarchive_seeding.core.interfaces import AuthProvider, HttpClient, RunContext
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

    def extract(self, ctx: RunContext) -> Iterator[DatasetRecord]:
        """Yield records, iterating over multiple queries if search_strategy is set."""
        strategy = ctx.config.source.search_strategy
        if strategy is None:
            # Legacy single-query mode
            query = ctx.config.source.params.get("q", "")
            yield from self._extract_single_query(ctx, str(query), query_string=str(query))
            return

        # Pre-populate seen_ids from existing records for resume support
        existing_ids = ctx.metadata.get("existing_dataset_ids")
        seen_ids: set[str] = set(existing_ids) if existing_ids else set()
        prefix = strategy.base_query_prefix
        facets = strategy.facet_filters
        bus = ctx.metadata.get("progress_bus")

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
                bus.publish(QueryProgress(
                    current_query=query_idx, total_queries=total_queries,
                    query_label=label, query_type="extension",
                ))
            yield from self._extract_with_date_splitting(
                ctx, query, seen_ids=seen_ids, query_string=label, extra_params=facets
            )

        # NL queries: combine into batched OR queries to avoid Zenodo timeouts.
        # Only facet params are used for filtering (not the prefix), because
        # Zenodo's facet param (e.g. type=dataset) returns far more results
        # than embedding resource_type.type:dataset in the q string.
        for batch_idx, batch in enumerate(nl_batches):
            query_idx += 1
            nl_clauses = " OR ".join(f"({nlq})" for nlq in batch)
            query = f"({nl_clauses})"
            label = f"nl batch {batch_idx + 1}/{len(nl_batches)} ({len(batch)} terms)"
            if bus:
                bus.publish(QueryProgress(
                    current_query=query_idx, total_queries=total_queries,
                    query_label=label, query_type="nl",
                ))
            yield from self._extract_with_date_splitting(
                ctx, query, seen_ids=seen_ids, query_string=label, extra_params=facets
            )

    def _extract_with_date_splitting(
        self,
        ctx: RunContext,
        query: str,
        *,
        seen_ids: set[str] | None = None,
        query_string: str = "",
        extra_params: dict[str, str] | None = None,
    ) -> Iterator[DatasetRecord]:
        """Extract records, splitting by date range if results exceed 10k."""
        if not self.options.auto_date_split:
            yield from self._extract_single_query(
                ctx, query, seen_ids=seen_ids,
                query_string=query_string, extra_params=extra_params,
            )
            return

        endpoint = ctx.config.source.endpoints.get("search", "/records")
        base_url = ctx.config.source.base_url.rstrip("/")
        url = f"{base_url}{endpoint}"

        # Build params for probing (same as _extract_single_query)
        probe_params: dict[str, Any] = dict(ctx.config.source.params)
        probe_params["q"] = query
        if extra_params:
            probe_params.update(extra_params)

        total = _probe_total(self.http_client, self.auth, url, probe_params)
        logger.info(
            "Query '%s' has %d total results", query_string, total,
        )

        if total <= _DATE_SPLIT_THRESHOLD:
            yield from self._extract_single_query(
                ctx, query, seen_ids=seen_ids,
                query_string=query_string, extra_params=extra_params,
            )
            return

        # Need date splitting
        today = date.today()
        slices = _find_date_slices(
            self.http_client, self.auth, url, probe_params,
            _ZENODO_EPOCH, today,
        )
        logger.info(
            "Query '%s' split into %d date slices to stay under %d results each",
            query_string, len(slices), _DATE_SPLIT_THRESHOLD,
        )

        bus = ctx.metadata.get("progress_bus")
        for slice_idx, (start, end) in enumerate(slices):
            date_query = f"{query} AND created:[{start} TO {end}]"
            slice_label = f"[{start}→{end}]"
            logger.debug(
                "Date slice %d/%d: %s %s",
                slice_idx + 1, len(slices), query_string, slice_label,
            )
            if bus:
                bus.publish(DateSliceProgress(
                    current_slice=slice_idx + 1,
                    total_slices=len(slices),
                    query_label=query_string,
                    slice_label=slice_label,
                ))
            yield from self._extract_single_query(
                ctx, date_query, seen_ids=seen_ids,
                query_string=f"{query_string} {slice_label}",
                extra_params=extra_params,
            )

    def _extract_single_query(
        self,
        ctx: RunContext,
        query: str,
        *,
        seen_ids: set[str] | None = None,
        query_string: str = "",
        extra_params: dict[str, str] | None = None,
    ) -> Iterator[DatasetRecord]:
        """Run a single paginated query against the Zenodo API."""
        endpoint = ctx.config.source.endpoints.get("search", "/records")
        base_url = ctx.config.source.base_url.rstrip("/")
        url = f"{base_url}{endpoint}"

        headers: dict[str, str] = {}
        params = dict(ctx.config.source.params)
        params["q"] = query
        if extra_params:
            params.update(extra_params)
        headers, params = self.auth.apply(headers, params)

        pagination = ctx.config.source.pagination
        paginator = PagePagination(
            page_param=(pagination.page_param if pagination and pagination.page_param else "page"),
            size_param=(pagination.size_param if pagination and pagination.size_param else "size"),
        )

        source_cfg = ctx.config.source
        effective_max_pages = self.options.max_pages or self.options._safety_max_pages
        total_yielded = 0
        bus = ctx.metadata.get("progress_bus")
        checkpoint = ctx.metadata.get("checkpoint")

        # Resume support: skip if this query was already completed
        if checkpoint is not None and checkpoint.is_query_complete(query_string):
            logger.info("Skipping completed query '%s' (checkpoint)", query_string)
            return

        # Resume support: skip already-fetched pages
        resume_from = checkpoint.get_start_page(query_string) if checkpoint else 0

        for page_count, page_params in enumerate(paginator.iter_params(params)):
            if page_count >= effective_max_pages:
                logger.warning("Reached max pages limit (%d), stopping", effective_max_pages)
                break
            if page_count < resume_from:
                continue  # Skip pages already checkpointed
            try:
                response = self.http_client.get(url, headers=headers, params=page_params)
            except Exception as exc:
                logger.error(
                    "HTTP request failed for query '%s' page %d: %s, skipping page",
                    query_string,
                    page_count + 1,
                    exc,
                )
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
                bus.publish(PageProgress(
                    current_page=page_count + 1,
                    total_hits=api_total,
                    query_label=query_string,
                ))
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
                record_id = str(item.get("id")) if item.get("id") is not None else None

                # Deduplicate across queries
                if seen_ids is not None and record_id:
                    if record_id in seen_ids:
                        continue
                    seen_ids.add(record_id)

                metadata = item.get("metadata", {})
                files = item.get("files", []) if self.options.include_files else []
                assets = [
                    AssetRecord(
                        asset_url=f.get("links", {}).get("self", ""),
                        local_filename=f.get("key"),
                        file_type=PurePosixPath(f.get("key", "")).suffix.lstrip(".")
                        if f.get("key")
                        else None,
                        size_bytes=f.get("size"),
                    )
                    for f in files
                    if f
                ]

                # Extract persons from creators + contributors
                persons: list[PersonRole] = []
                for creator in metadata.get("creators", []):
                    if creator.get("name"):
                        persons.append(
                            PersonRole(name=creator["name"], role=PERSON_ROLE_CREATOR)
                        )
                for contributor in metadata.get("contributors", []):
                    if contributor.get("name"):
                        persons.append(
                            PersonRole(name=contributor["name"], role=PERSON_ROLE_CONTRIBUTOR)
                        )

                yield DatasetRecord(
                    source_name=source_cfg.name,
                    source_dataset_id=record_id,
                    source_url=item.get("links", {}).get("self", url),
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
                    # Deprecated fields (kept for backward compat)
                    year=_extract_year(metadata.get("publication_date")),
                    owner_name=metadata.get("creators", [{}])[0].get("name")
                    if metadata.get("creators")
                    else None,
                    assets=assets,
                    raw=item,
                )

        # Mark query as complete after all pages fetched
        if checkpoint is not None:
            checkpoint.mark_query_complete(query_string)


def _batched(items: list[str], size: int) -> list[list[str]]:
    """Split *items* into sub-lists of at most *size* elements."""
    if not items:
        return []
    return [items[i : i + size] for i in range(0, len(items), size)]


def _probe_total(
    http_client: HttpClient,
    auth: AuthProvider,
    url: str,
    base_params: dict[str, Any],
) -> int:
    """Probe the total hit count for a query using size=1 (cheapest request)."""
    headers: dict[str, str] = {}
    params = {**base_params, "size": 1, "page": 1}
    headers, params = auth.apply(headers, params)
    try:
        resp = http_client.get(url, headers=headers, params=params)
        return resp.json().get("hits", {}).get("total", 0)
    except Exception as exc:
        logger.warning("Probe request failed: %s", exc)
        return 0


def _find_date_slices(
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

    total = _probe_total(http_client, auth, url, params)

    if total == 0:
        return []
    if total <= threshold:
        return [(start, end)]
    if start >= end:
        # Single day still over threshold — can't split further, accept the cap
        logger.warning(
            "Single day %s has %d results (> %d), accepting 10k cap",
            start, total, threshold,
        )
        return [(start, end)]

    # Split in half
    mid = start + (end - start) // 2
    logger.debug(
        "Splitting [%s, %s] (%d results) into [%s, %s] + [%s, %s]",
        start, end, total, start, mid, mid + timedelta(days=1), end,
    )
    left = _find_date_slices(
        http_client, auth, url, base_params, start, mid, threshold
    )
    right = _find_date_slices(
        http_client, auth, url, base_params, mid + timedelta(days=1), end, threshold
    )
    return left + right


def _extract_license(value: object) -> str | None:
    """Zenodo returns license as a dict like {"id": "cc-by-4.0"} or a string."""
    if value is None:
        return None
    if isinstance(value, dict):
        return str(value.get("id", value.get("title", "")))
    return str(value)


def _extract_year(publication_date: object) -> int | None:
    """Extract year as int from a publication_date string like '2024-01-15'."""
    if not publication_date or not isinstance(publication_date, str):
        return None
    try:
        return int(publication_date[:4])
    except (ValueError, IndexError):
        return None
