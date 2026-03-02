from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.core.interfaces import AuthProvider, HttpClient, RunContext
from qdarchive_seeding.infra.http.pagination import (
    CursorPagination,
    OffsetPagination,
    PagePagination,
    PaginationType,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class GenericRestOptions:
    records_path: str = "items"
    max_pages: int | None = None
    _safety_max_pages: int = 200


@dataclass(slots=True)
class GenericRestExtractor:
    http_client: HttpClient
    auth: AuthProvider
    options: GenericRestOptions

    def _select_pagination(self, pagination_type: PaginationType | None, ctx: RunContext) -> Any:
        pagination = ctx.config.source.pagination
        if pagination_type == "offset":
            return OffsetPagination(
                offset_param=(pagination.offset_param if pagination else None) or "offset",
                size_param=(pagination.size_param if pagination else None) or "limit",
            )
        if pagination_type == "cursor":
            return CursorPagination(
                cursor_param=(pagination.cursor_param if pagination else None) or "cursor"
            )
        return PagePagination(
            page_param=(pagination.page_param if pagination else None) or "page",
            size_param=(pagination.size_param if pagination else None) or "size",
        )

    def extract(self, ctx: RunContext) -> Iterator[DatasetRecord]:
        """Yield records page-by-page so the runner can stop when enough pass filters."""
        endpoint = ctx.config.source.endpoints.get("search", "")
        base_url = ctx.config.source.base_url.rstrip("/")
        url = f"{base_url}{endpoint}"

        headers: dict[str, str] = {}
        params = dict(ctx.config.source.params)
        headers, params = self.auth.apply(headers, params)

        pagination_type = (
            ctx.config.source.pagination.type if ctx.config.source.pagination else None
        )
        paginator = self._select_pagination(pagination_type, ctx)

        total_yielded = 0
        effective_max_pages = self.options.max_pages or self.options._safety_max_pages
        page_count = 0
        for page_params in paginator.iter_params(params):
            if page_count >= effective_max_pages:
                logger.warning("Reached max pages limit (%d), stopping", effective_max_pages)
                break
            response = self.http_client.get(url, headers=headers, params=page_params)
            response.raise_for_status()
            payload = response.json()
            items = payload
            for part in self.options.records_path.split("."):
                if isinstance(items, dict):
                    items = items.get(part, [])
            if not isinstance(items, list):
                break
            if not items:
                break
            total_yielded += len(items)
            logger.info(
                "Page %d: fetched %d items (%d total)", page_count + 1, len(items), total_yielded
            )
            for item in items:
                if not isinstance(item, dict):
                    continue
                yield DatasetRecord(
                    source_name=ctx.config.source.name,
                    source_dataset_id=str(item.get("id")) if item.get("id") is not None else None,
                    source_url=str(item.get("url") or url),
                    title=item.get("title"),
                    description=item.get("description"),
                    assets=[
                        AssetRecord(asset_url=str(asset))
                        for asset in item.get("assets", [])
                        if asset is not None
                    ],
                    raw=item,
                )
            page_count += 1
