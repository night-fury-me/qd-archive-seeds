from __future__ import annotations

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


@dataclass(slots=True)
class GenericRestOptions:
    records_path: str = "items"
    max_pages: int | None = None


@dataclass(slots=True)
class GenericRestExtractor:
    http_client: HttpClient
    auth: AuthProvider
    options: GenericRestOptions

    def _select_pagination(self, pagination_type: PaginationType | None, ctx: RunContext) -> Any:
        if pagination_type == "offset":
            return OffsetPagination(
                offset_param=ctx.config.source.pagination.offset_param or "offset",
                size_param=ctx.config.source.pagination.size_param or "limit",
            )
        if pagination_type == "cursor":
            return CursorPagination(
                cursor_param=ctx.config.source.pagination.cursor_param or "cursor"
            )
        return PagePagination(
            page_param=ctx.config.source.pagination.page_param or "page",
            size_param=ctx.config.source.pagination.size_param or "size",
        )

    def extract(self, ctx: RunContext) -> list[DatasetRecord]:
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

        records: list[DatasetRecord] = []
        page_count = 0
        for page_params in paginator.iter_params(params):
            response = self.http_client.get(url, headers=headers, params=page_params)
            response.raise_for_status()
            payload = response.json()
            items = payload
            for part in self.options.records_path.split("."):
                if isinstance(items, dict):
                    items = items.get(part, [])
            if not isinstance(items, list):
                break
            for item in items:
                if not isinstance(item, dict):
                    continue
                record = DatasetRecord(
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
                records.append(record)
            page_count += 1
            if self.options.max_pages and page_count >= self.options.max_pages:
                break
        return records
