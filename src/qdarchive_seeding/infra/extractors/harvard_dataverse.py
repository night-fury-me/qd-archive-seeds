from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any

from qdarchive_seeding.app.progress import PageProgress, QueryProgress
from qdarchive_seeding.core.constants import (
    DOWNLOAD_METHOD_API,
    PERSON_ROLE_CREATOR,
)
from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord, PersonRole
from qdarchive_seeding.core.interfaces import AuthProvider, HttpClient, RunContext

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class HarvardDataverseOptions:
    include_files: bool = True
    max_pages: int | None = None
    per_page: int = 10
    _safety_max_pages: int = 200


@dataclass(slots=True)
class HarvardDataverseExtractor:
    """Extractor for Harvard Dataverse (and compatible Dataverse installations).

    Uses the Dataverse Search API (offset-based pagination) and the
    Dataset Files API to retrieve per-dataset file listings.
    """

    http_client: HttpClient
    auth: AuthProvider
    options: HarvardDataverseOptions

    async def extract(self, ctx: RunContext) -> AsyncIterator[DatasetRecord]:
        """Yield records, iterating over multiple queries if search_strategy is set."""
        strategy = ctx.config.source.search_strategy
        if strategy is None:
            query = ctx.config.source.params.get("q", "")
            async for record in self._extract_single_query(
                ctx, str(query), query_string=str(query)
            ):
                yield record
            return

        existing_ids = ctx.metadata.get("existing_dataset_ids")
        seen_ids: set[str] = set(existing_ids) if existing_ids else set()
        prefix = strategy.base_query_prefix
        bus = ctx.metadata.get("progress_bus")
        total_queries = len(strategy.extension_queries) + len(strategy.natural_language_queries)
        query_idx = 0

        for ext in strategy.extension_queries:
            query_idx += 1
            query = f"{prefix} {ext}".strip() if prefix else ext
            if bus:
                bus.publish(
                    QueryProgress(
                        current_query=query_idx,
                        total_queries=total_queries,
                        query_label=ext,
                        query_type="extension",
                    )
                )
            async for record in self._extract_single_query(
                ctx, query, seen_ids=seen_ids, query_string=ext
            ):
                yield record

        for nl_query in strategy.natural_language_queries:
            query_idx += 1
            query = f"{prefix} {nl_query}".strip() if prefix else nl_query
            if bus:
                bus.publish(
                    QueryProgress(
                        current_query=query_idx,
                        total_queries=total_queries,
                        query_label=nl_query,
                        query_type="nl",
                    )
                )
            async for record in self._extract_single_query(
                ctx, query, seen_ids=seen_ids, query_string=nl_query
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
        """Run a single paginated query against the Dataverse Search API."""
        base_url = ctx.config.source.base_url.rstrip("/")
        search_endpoint = ctx.config.source.endpoints.get("search", "/search")
        url = f"{base_url}{search_endpoint}"

        headers: dict[str, str] = {}
        params: dict[str, Any] = {"q": query, "type": "dataset"}
        headers, params = self.auth.apply(headers, params)

        source_cfg = ctx.config.source
        per_page = self.options.per_page
        effective_max_pages = self.options.max_pages or self.options._safety_max_pages
        bus = ctx.metadata.get("progress_bus")
        checkpoint = ctx.metadata.get("checkpoint")

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
                for failed_page in list(failed_pages):
                    retry_start = failed_page * per_page
                    retry_params = {**params, "start": retry_start, "per_page": per_page}
                    try:
                        response = await self.http_client.get(
                            url, headers=headers, params=retry_params
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
                    items = payload.get("data", {}).get("items", [])
                    checkpoint.clear_failed_page(query_string, failed_page)
                    if not items:
                        continue
                    logger.info(
                        "Retry succeeded for query '%s' page %d: %d items",
                        query_string,
                        failed_page + 1,
                        len(items),
                    )
                    for item in items:
                        record = await self._build_record(
                            ctx, item, source_cfg, query_string, seen_ids
                        )
                        if record is not None:
                            yield record

        # Resume support: skip already-fetched pages
        resume_from = checkpoint.get_start_page(query_string) if checkpoint else 0
        start = resume_from * per_page

        for page_count in range(resume_from, effective_max_pages):
            page_params = {**params, "start": start, "per_page": per_page}
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
                start += per_page
                continue
            payload = response.json()

            data = payload.get("data", {})
            items = data.get("items", [])
            total_count = data.get("total_count", 0)
            if not isinstance(items, list) or not items:
                break

            # Checkpoint after successful page fetch
            if checkpoint is not None:
                checkpoint.mark_page(query_string, page_count + 1, len(items))

            if bus:
                bus.publish(
                    PageProgress(
                        current_page=page_count + 1,
                        total_hits=total_count,
                        query_label=query_string,
                    )
                )
            logger.debug(
                "Query '%s' page %d: fetched %d items (start=%d)",
                query_string,
                page_count + 1,
                len(items),
                start,
            )

            for item in items:
                record = await self._build_record(
                    ctx, item, source_cfg, query_string, seen_ids
                )
                if record is not None:
                    yield record

            # Advance offset
            start += per_page
            if start >= total_count:
                break

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

    async def _build_record(
        self,
        ctx: RunContext,
        item: dict[str, Any],
        source_cfg: Any,
        query_string: str,
        seen_ids: set[str] | None,
    ) -> DatasetRecord | None:
        """Build a DatasetRecord from a Dataverse search result. Returns None if duplicate."""
        global_id = item.get("global_id")
        if not global_id:
            return None

        if seen_ids is not None:
            if global_id in seen_ids:
                return None
            seen_ids.add(global_id)

        assets = await self._fetch_files(ctx, global_id) if self.options.include_files else []

        persons: list[PersonRole] = []
        for author in item.get("authors", []):
            if isinstance(author, str) and author:
                persons.append(PersonRole(name=author, role=PERSON_ROLE_CREATOR))

        dataset_url = item.get("url", "")
        project_folder = global_id.replace("doi:", "").replace("/", "_")

        return DatasetRecord(
            source_name=source_cfg.name,
            source_dataset_id=global_id,
            source_url=dataset_url,
            title=item.get("name"),
            description=item.get("description"),
            doi=global_id if global_id.startswith("doi:") else None,
            license=None,
            query_string=query_string,
            repository_id=source_cfg.repository_id,
            repository_url=source_cfg.repository_url,
            language=None,
            upload_date=item.get("published_at"),
            download_method=DOWNLOAD_METHOD_API,
            download_repository_folder=source_cfg.name,
            download_project_folder=project_folder,
            keywords=item.get("keywords", []),
            persons=persons,
            assets=assets,
            raw=item,
        )

    async def _fetch_files(self, ctx: RunContext, persistent_id: str) -> list[AssetRecord]:
        """Fetch the file listing for a dataset via the Dataverse Files API."""
        base_url = ctx.config.source.base_url.rstrip("/")
        files_endpoint = ctx.config.source.endpoints.get(
            "files", "/datasets/:persistentId/versions/:latest/files"
        )
        # The Dataverse Files API uses :persistentId as a literal path token;
        # the actual DOI is passed as the ?persistentId= query parameter.
        url = f"{base_url}{files_endpoint}"

        headers: dict[str, str] = {}
        params: dict[str, Any] = {"persistentId": persistent_id}
        headers, params = self.auth.apply(headers, params)

        try:
            response = await self.http_client.get(url, headers=headers, params=params)
            payload = response.json()
        except Exception as exc:
            logger.debug("Failed to fetch files for %s: %s", persistent_id, exc)
            return []

        data = payload.get("data", [])
        if not isinstance(data, list):
            return []

        assets: list[AssetRecord] = []
        for file_entry in data:
            data_file = file_entry.get("dataFile", {})
            filename = data_file.get("filename", "")
            file_id = data_file.get("id")
            if not filename or not file_id:
                continue

            download_url = f"{ctx.config.source.base_url.rstrip('/')}/access/datafile/{file_id}"
            file_type = PurePosixPath(filename).suffix.lstrip(".") if filename else None

            assets.append(
                AssetRecord(
                    asset_url=download_url,
                    local_filename=filename,
                    file_type=file_type,
                    size_bytes=data_file.get("filesize"),
                )
            )

        return assets
