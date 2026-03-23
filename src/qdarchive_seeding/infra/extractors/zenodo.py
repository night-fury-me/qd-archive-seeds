from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import PurePosixPath

from qdarchive_seeding.core.constants import (
    DOWNLOAD_METHOD_API,
    PERSON_ROLE_CONTRIBUTOR,
    PERSON_ROLE_CREATOR,
)
from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord, PersonRole
from qdarchive_seeding.core.interfaces import AuthProvider, HttpClient, RunContext
from qdarchive_seeding.infra.http.pagination import PagePagination

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ZenodoOptions:
    include_files: bool = True
    max_pages: int | None = None
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

        seen_ids: set[str] = set()
        prefix = strategy.base_query_prefix
        facets = strategy.facet_filters

        # Extension-based queries: use prefix in query string + facet params
        for i, ext in enumerate(strategy.extension_queries, 1):
            query = f"{prefix} filetype:{ext}".strip() if prefix else f"filetype:{ext}"
            logger.info(
                "Extension query %d/%d: %s",
                i, len(strategy.extension_queries), ext,
            )
            yield from self._extract_single_query(
                ctx, query, seen_ids=seen_ids, query_string=ext, extra_params=facets
            )

        # NL queries: use only facet params for filtering (not the prefix),
        # because Zenodo's facet param (e.g. type=dataset) returns far more
        # results than embedding resource_type.type:dataset in the q string.
        for i, nl_query in enumerate(strategy.natural_language_queries, 1):
            logger.info(
                "NL query %d/%d: %s",
                i, len(strategy.natural_language_queries), nl_query,
            )
            yield from self._extract_single_query(
                ctx, nl_query, seen_ids=seen_ids, query_string=nl_query, extra_params=facets
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

        for page_count, page_params in enumerate(paginator.iter_params(params)):
            if page_count >= effective_max_pages:
                logger.warning("Reached max pages limit (%d), stopping", effective_max_pages)
                break
            logger.debug(
                "Fetching page %d for query '%s' ...", page_count + 1, query_string
            )
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
            hits = payload.get("hits", {}).get("hits", [])
            if not isinstance(hits, list):
                break
            if not hits:
                break
            total_yielded += len(hits)
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
