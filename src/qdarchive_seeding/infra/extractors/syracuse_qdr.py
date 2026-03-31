from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

from qdarchive_seeding.app.progress import PageProgress, QueryProgress
from qdarchive_seeding.core.constants import (
    DOWNLOAD_METHOD_API,
    PERSON_ROLE_CREATOR,
)
from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord, PersonRole
from qdarchive_seeding.core.interfaces import AuthProvider, HttpClient, RunContext
from qdarchive_seeding.infra.http.auth import apply_auth_async
from qdarchive_seeding.infra.http.pagination import OffsetPagination

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class SyracuseQdrOptions:
    max_datasets: int | None = None
    include_files: bool = True
    _safety_max_datasets: int = 5000


@dataclass(slots=True)
class SyracuseQdrExtractor:
    """Two-phase extractor for Syracuse QDR (Dataverse-based).

    Phase 1: search datasets via ``/search?q=...&type=dataset``.
    Phase 2: for each dataset, fetch the full file listing via
             ``/datasets/:persistentId/?persistentId={doi}``.

    Supports checkpoint/resume and progress reporting, consistent with
    the Zenodo and Harvard Dataverse extractors.
    """

    http_client: HttpClient
    auth: AuthProvider
    options: SyracuseQdrOptions

    async def extract(self, ctx: RunContext) -> AsyncIterator[DatasetRecord]:
        """Yield records, iterating over search_strategy queries if configured."""
        strategy = ctx.config.source.search_strategy
        if strategy is None:
            # Legacy single-query mode
            query = str(ctx.config.source.params.get("q", "*"))
            async for record in self._extract_single_query(ctx, query, query_string=query):
                yield record
            return

        bus = ctx.metadata.get("progress_bus")
        seen_ids: set[str] = set()

        # Pre-populate seen_ids from existing records for resume support
        existing_ids = ctx.metadata.get("existing_dataset_ids")
        if existing_ids:
            seen_ids.update(existing_ids)

        # Extension queries — Syracuse QDR is small, so no batching needed
        queries: list[tuple[str, str]] = []
        for ext in strategy.extension_queries:
            queries.append((ext, "extension"))
        for nlq in strategy.natural_language_queries:
            queries.append((nlq, "nl"))

        total_queries = len(queries)
        for query_idx, (query_text, query_type) in enumerate(queries, 1):
            label = f"{query_type}: {query_text}"
            if bus:
                bus.publish(
                    QueryProgress(
                        current_query=query_idx,
                        total_queries=total_queries,
                        query_label=label,
                        query_type=query_type,
                    )
                )
            async for record in self._extract_single_query(
                ctx, query_text, seen_ids=seen_ids, query_string=label
            ):
                yield record

    async def _extract_single_query(
        self,
        ctx: RunContext,
        query: str,
        *,
        seen_ids: set[str] | None = None,
        query_string: str = "",
    ) -> AsyncIterator[DatasetRecord]:
        """Run a single paginated query against the Syracuse QDR search API."""
        base_url = ctx.config.source.base_url.rstrip("/")
        search_endpoint = ctx.config.source.endpoints.get("search", "/search")
        dataset_endpoint = ctx.config.source.endpoints.get("dataset", "/datasets/:persistentId/")
        search_url = f"{base_url}{search_endpoint}"

        headers: dict[str, str] = {}
        params = dict(ctx.config.source.params)
        params["q"] = query
        headers, params = await apply_auth_async(self.auth, headers, params)

        pagination = ctx.config.source.pagination
        paginator = OffsetPagination(
            offset_param=(
                pagination.offset_param if pagination and pagination.offset_param else "start"
            ),
            size_param=(
                pagination.size_param if pagination and pagination.size_param else "per_page"
            ),
            start_offset=0,
        )

        effective_max = self.options.max_datasets or self.options._safety_max_datasets
        datasets_yielded = 0
        bus = ctx.metadata.get("progress_bus")
        checkpoint = ctx.metadata.get("checkpoint")
        source_cfg = ctx.config.source

        # Resume support: skip if this query was already completed
        if checkpoint is not None and checkpoint.is_query_complete(query_string):
            logger.info("Skipping completed query '%s' (checkpoint)", query_string)
            return

        # Resume support: skip already-fetched pages
        resume_from = checkpoint.get_start_page(query_string) if checkpoint else 0

        for page_count, page_params in enumerate(paginator.iter_params(params)):
            if datasets_yielded >= effective_max:
                logger.warning("Reached max datasets limit (%d), stopping", effective_max)
                break
            if page_count < resume_from:
                continue

            try:
                response = await self.http_client.get(
                    search_url, headers=headers, params=page_params, timeout=90.0
                )
                payload = response.json()
            except Exception as exc:
                logger.error(
                    "HTTP request failed for query '%s' page %d: %s",
                    query_string,
                    page_count + 1,
                    exc,
                )
                if checkpoint is not None:
                    checkpoint.mark_page_failed(query_string, page_count)
                continue

            data = payload.get("data", {})
            items = data.get("items", [])
            total_count = data.get("total_count", 0)

            if not isinstance(items, list) or not items:
                break

            if bus:
                bus.publish(
                    PageProgress(
                        current_page=page_count + 1,
                        total_hits=total_count,
                        query_label=query_string,
                    )
                )

            # Checkpoint after successful page fetch
            if checkpoint is not None:
                checkpoint.mark_page(query_string, page_count + 1, len(items))

            logger.debug(
                "Query '%s' page %d: fetched %d datasets (%d/%d total)",
                query_string,
                page_count + 1,
                len(items),
                datasets_yielded + len(items),
                total_count,
            )

            for item in items:
                if datasets_yielded >= effective_max:
                    break

                global_id = item.get("global_id", "")
                if not global_id:
                    logger.debug("Skipping dataset without global_id: %s", item.get("name"))
                    continue

                if seen_ids is not None:
                    if global_id in seen_ids:
                        continue
                    seen_ids.add(global_id)

                record = await self._build_record(
                    ctx,
                    item,
                    global_id,
                    base_url,
                    dataset_endpoint,
                    headers,
                    source_cfg,
                    query_string,
                )
                if record is not None:
                    datasets_yielded += 1
                    yield record

            if datasets_yielded >= total_count:
                logger.info("All %d datasets fetched for query '%s'", total_count, query_string)
                break

        # Mark query complete if no failures
        if checkpoint is not None:
            if not checkpoint.get_failed_pages(query_string):
                checkpoint.mark_query_complete(query_string)
            else:
                logger.warning(
                    "Query '%s' has %d failed pages, not marking complete",
                    query_string,
                    len(checkpoint.get_failed_pages(query_string)),
                )

    async def _build_record(
        self,
        ctx: RunContext,
        item: dict[str, Any],
        global_id: str,
        base_url: str,
        dataset_endpoint: str,
        headers: dict[str, str],
        source_cfg: Any,
        query_string: str,
    ) -> DatasetRecord | None:
        """Build a DatasetRecord from a search result, fetching files if configured."""
        assets: list[AssetRecord] = []
        license_name: str | None = None

        if self.options.include_files:
            result = await self._fetch_dataset_files(base_url, dataset_endpoint, global_id, headers)
            if result is not None:
                file_list, license_name = result
                # Capture auth headers so the downloader can apply them per-request
                auth_headers, _ = await apply_auth_async(self.auth, {}, {})
                asset_meta = {"auth_headers": auth_headers} if auth_headers else None
                assets = [
                    AssetRecord(
                        asset_url=f"{base_url}/access/datafile/{df['id']}",
                        local_filename=df.get("filename", ""),
                        file_type=_extract_extension(df.get("filename", "")),
                        size_bytes=df.get("filesize"),
                        metadata=asset_meta,
                    )
                    for df in file_list
                    if df.get("id") is not None
                ]

        doi = global_id.removeprefix("doi:") if global_id.startswith("doi:") else global_id

        persons: list[PersonRole] = []
        for author in item.get("authors", []):
            if isinstance(author, str) and author:
                persons.append(PersonRole(name=author, role=PERSON_ROLE_CREATOR))

        return DatasetRecord(
            source_name=source_cfg.name,
            source_dataset_id=global_id,
            source_url=(f"{base_url.rsplit('/api', 1)[0]}/dataset.xhtml?persistentId={global_id}"),
            title=item.get("name"),
            description=item.get("description"),
            doi=doi,
            license=license_name,
            query_string=query_string,
            repository_id=source_cfg.repository_id,
            repository_url=source_cfg.repository_url,
            version=None,  # QDR doesn't expose version in search results
            language=None,
            upload_date=item.get("published_at"),
            download_method=DOWNLOAD_METHOD_API,
            download_repository_folder=source_cfg.name,
            download_project_folder=global_id,
            keywords=item.get("subjects", []),
            persons=persons,
            year=_extract_year(item.get("published_at")),
            owner_name=_first_author(item.get("authors")),
            assets=assets,
            raw=item,
        )

    async def _fetch_dataset_files(
        self,
        base_url: str,
        dataset_endpoint: str,
        persistent_id: str,
        headers: dict[str, str],
    ) -> tuple[list[dict[str, Any]], str | None] | None:
        """Fetch the file listing for a single dataset.

        Returns ``(list_of_dataFile_dicts, license_name)`` or ``None`` on error.
        """
        # :persistentId is a literal path token; actual ID goes in query param
        url = f"{base_url}{dataset_endpoint}"
        params: dict[str, Any] = {"persistentId": persistent_id}

        try:
            response = await self.http_client.get(url, headers=headers, params=params, timeout=90.0)
            payload = response.json()
        except Exception as exc:
            logger.debug("Failed to fetch files for dataset %s: %s", persistent_id, exc)
            return None

        data = payload.get("data", {})
        latest = data.get("latestVersion", {})

        license_info = latest.get("license", {})
        license_name: str | None = None
        if isinstance(license_info, dict):
            license_name = license_info.get("name")
        elif isinstance(license_info, str):
            license_name = license_info

        raw_files = latest.get("files", [])
        datafiles: list[dict[str, Any]] = []
        for entry in raw_files:
            df = entry.get("dataFile")
            if isinstance(df, dict):
                datafiles.append(df)

        logger.debug("Dataset %s: %d files", persistent_id, len(datafiles))
        return datafiles, license_name


def _extract_year(published_at: object) -> int | None:
    """Extract year from an ISO timestamp like ``2023-11-01T18:47:35Z``."""
    if not published_at or not isinstance(published_at, str):
        return None
    try:
        return int(published_at[:4])
    except (ValueError, IndexError):
        return None


def _first_author(authors: object) -> str | None:
    """Return the first author name from a list like ``["Smith, John", ...]``."""
    if isinstance(authors, list) and authors:
        return str(authors[0])
    return None


def _extract_extension(filename: str) -> str | None:
    """Extract file extension from a filename."""
    if "." in filename:
        return filename.rsplit(".", 1)[-1].lower()
    return None
