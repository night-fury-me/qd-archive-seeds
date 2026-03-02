from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
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
        """Yield records page-by-page so the runner can stop when enough pass filters."""
        endpoint = ctx.config.source.endpoints.get("search", "/records")
        base_url = ctx.config.source.base_url.rstrip("/")
        url = f"{base_url}{endpoint}"

        headers: dict[str, str] = {}
        params = dict(ctx.config.source.params)
        headers, params = self.auth.apply(headers, params)

        pagination = ctx.config.source.pagination
        paginator = PagePagination(
            page_param=(pagination.page_param if pagination and pagination.page_param else "page"),
            size_param=(pagination.size_param if pagination and pagination.size_param else "size"),
        )

        total_yielded = 0
        effective_max_pages = self.options.max_pages or self.options._safety_max_pages
        for page_count, page_params in enumerate(paginator.iter_params(params)):
            if page_count >= effective_max_pages:
                logger.warning("Reached max pages limit (%d), stopping", effective_max_pages)
                break
            response = self.http_client.get(url, headers=headers, params=page_params)
            payload = response.json()
            hits = payload.get("hits", {}).get("hits", [])
            if not isinstance(hits, list):
                break
            if not hits:
                break
            total_yielded += len(hits)
            logger.info(
                "Page %d: fetched %d hits (%d total)", page_count + 1, len(hits), total_yielded
            )
            for item in hits:
                metadata = item.get("metadata", {})
                files = item.get("files", []) if self.options.include_files else []
                assets = [
                    AssetRecord(
                        asset_url=f.get("links", {}).get("self", ""),
                        local_filename=f.get("key"),
                    )
                    for f in files
                    if f
                ]
                yield DatasetRecord(
                    source_name=ctx.config.source.name,
                    source_dataset_id=str(item.get("id")) if item.get("id") is not None else None,
                    source_url=item.get("links", {}).get("self", url),
                    title=metadata.get("title"),
                    description=metadata.get("description"),
                    doi=metadata.get("doi"),
                    license=_extract_license(metadata.get("license")),
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
