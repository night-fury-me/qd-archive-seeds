from __future__ import annotations

import logging
from collections.abc import Iterator
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

    def extract(self, ctx: RunContext) -> Iterator[DatasetRecord]:
        """Yield records, iterating over multiple queries if search_strategy is set."""
        strategy = ctx.config.source.search_strategy
        if strategy is None:
            query = ctx.config.source.params.get("q", "")
            yield from self._extract_single_query(ctx, str(query), query_string=str(query))
            return

        seen_ids: set[str] = set()
        prefix = strategy.base_query_prefix
        bus = ctx.metadata.get("progress_bus")
        total_queries = len(strategy.extension_queries) + len(strategy.natural_language_queries)
        query_idx = 0

        for ext in strategy.extension_queries:
            query_idx += 1
            query = f"{prefix} {ext}".strip() if prefix else ext
            if bus:
                bus.publish(QueryProgress(
                    current_query=query_idx, total_queries=total_queries,
                    query_label=ext, query_type="extension",
                ))
            yield from self._extract_single_query(
                ctx, query, seen_ids=seen_ids, query_string=ext
            )

        for nl_query in strategy.natural_language_queries:
            query_idx += 1
            query = f"{prefix} {nl_query}".strip() if prefix else nl_query
            if bus:
                bus.publish(QueryProgress(
                    current_query=query_idx, total_queries=total_queries,
                    query_label=nl_query, query_type="nl",
                ))
            yield from self._extract_single_query(
                ctx, query, seen_ids=seen_ids, query_string=nl_query
            )

    def _extract_single_query(
        self,
        ctx: RunContext,
        query: str,
        *,
        seen_ids: set[str] | None = None,
        query_string: str = "",
    ) -> Iterator[DatasetRecord]:
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
        start = 0
        bus = ctx.metadata.get("progress_bus")

        for page_count in range(effective_max_pages):
            page_params = {**params, "start": start, "per_page": per_page}
            try:
                response = self.http_client.get(url, headers=headers, params=page_params)
            except Exception as exc:
                logger.error(
                    "HTTP request failed for query '%s' page %d: %s",
                    query_string,
                    page_count + 1,
                    exc,
                )
                break
            payload = response.json()

            data = payload.get("data", {})
            items = data.get("items", [])
            total_count = data.get("total_count", 0)
            if not isinstance(items, list) or not items:
                break

            if bus:
                bus.publish(PageProgress(
                    current_page=page_count + 1,
                    total_hits=total_count,
                    query_label=query_string,
                ))
            logger.debug(
                "Query '%s' page %d: fetched %d items (start=%d)",
                query_string,
                page_count + 1,
                len(items),
                start,
            )

            for item in items:
                global_id = item.get("global_id")
                if not global_id:
                    continue

                # Deduplicate across queries
                if seen_ids is not None:
                    if global_id in seen_ids:
                        continue
                    seen_ids.add(global_id)

                # Fetch files for this dataset
                assets = self._fetch_files(ctx, global_id) if self.options.include_files else []

                # Extract persons from authors string
                persons: list[PersonRole] = []
                for author in item.get("authors", []):
                    if isinstance(author, str) and author:
                        persons.append(PersonRole(name=author, role=PERSON_ROLE_CREATOR))

                # Extract version from the dataset URL or metadata
                dataset_url = item.get("url", "")
                # Dataverse persistent IDs look like "doi:10.7910/DVN/XXXXX"
                project_folder = global_id.replace("doi:", "").replace("/", "_")

                yield DatasetRecord(
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

            # Advance offset
            total_count = data.get("total_count", 0)
            start += per_page
            if start >= total_count:
                break

    def _fetch_files(self, ctx: RunContext, persistent_id: str) -> list[AssetRecord]:
        """Fetch the file listing for a dataset via the Dataverse Files API."""
        base_url = ctx.config.source.base_url.rstrip("/")
        files_endpoint = ctx.config.source.endpoints.get(
            "files", "/datasets/:persistentId/versions/:latest/files"
        )
        url = f"{base_url}{files_endpoint}"
        url = url.replace(":persistentId", persistent_id).replace(":latest", ":latest")

        headers: dict[str, str] = {}
        params: dict[str, Any] = {"persistentId": persistent_id}
        headers, params = self.auth.apply(headers, params)

        try:
            response = self.http_client.get(url, headers=headers, params=params)
            payload = response.json()
        except Exception:
            logger.warning("Failed to fetch files for %s", persistent_id, exc_info=True)
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
