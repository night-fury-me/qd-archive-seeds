from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.core.interfaces import AuthProvider, HttpClient, RunContext
from qdarchive_seeding.infra.http.pagination import PagePagination


@dataclass(slots=True)
class ZenodoOptions:
    include_files: bool = True
    max_pages: int | None = None


@dataclass(slots=True)
class ZenodoExtractor:
    http_client: HttpClient
    auth: AuthProvider
    options: ZenodoOptions

    def extract(self, ctx: RunContext) -> list[DatasetRecord]:
        endpoint = ctx.config.source.endpoints.get("search", "/records")
        base_url = ctx.config.source.base_url.rstrip("/")
        url = f"{base_url}{endpoint}"

        headers: dict[str, str] = {}
        params = dict(ctx.config.source.params)
        headers, params = self.auth.apply(headers, params)

        paginator = PagePagination(
            page_param=ctx.config.source.pagination.page_param if ctx.config.source.pagination else "page",
            size_param=ctx.config.source.pagination.size_param if ctx.config.source.pagination else "size",
        )

        records: list[DatasetRecord] = []
        page_count = 0
        for page_params in paginator.iter_params(params):
            response = self.http_client.get(url, headers=headers, params=page_params)
            response.raise_for_status()
            payload = response.json()
            hits = payload.get("hits", {}).get("hits", [])
            if not isinstance(hits, list):
                break
            for item in hits:
                metadata = item.get("metadata", {})
                files = item.get("files", []) if self.options.include_files else []
                assets = [AssetRecord(asset_url=f.get("links", {}).get("self", "")) for f in files if f]
                record = DatasetRecord(
                    source_name=ctx.config.source.name,
                    source_dataset_id=str(item.get("id")) if item.get("id") is not None else None,
                    source_url=item.get("links", {}).get("self", url),
                    title=metadata.get("title"),
                    description=metadata.get("description"),
                    doi=metadata.get("doi"),
                    license=metadata.get("license"),
                    year=metadata.get("publication_date", "")[:4] if metadata.get("publication_date") else None,
                    owner_name=metadata.get("creators", [{}])[0].get("name") if metadata.get("creators") else None,
                    assets=assets,
                    raw=item,
                )
                records.append(record)
            page_count += 1
            if self.options.max_pages and page_count >= self.options.max_pages:
                break
        return records
