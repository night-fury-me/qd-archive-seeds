from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass

from bs4 import BeautifulSoup

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.core.interfaces import HttpClient, RunContext


@dataclass(slots=True)
class HtmlScraperOptions:
    list_selector: str
    title_selector: str
    link_selector: str
    description_selector: str | None = None
    asset_selector: str | None = None
    max_items: int | None = None


@dataclass(slots=True)
class HtmlScraperExtractor:
    http_client: HttpClient
    options: HtmlScraperOptions

    async def extract(self, ctx: RunContext) -> AsyncIterator[DatasetRecord]:
        endpoint = ctx.config.source.endpoints.get("search", "")
        base_url = ctx.config.source.base_url.rstrip("/")
        url = f"{base_url}{endpoint}"

        response = await self.http_client.get(url, headers={}, params={})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        items = soup.select(self.options.list_selector)
        count = 0
        for item in items:
            title_node = item.select_one(self.options.title_selector)
            link_node = item.select_one(self.options.link_selector)
            if not title_node or not link_node:
                continue
            description = None
            if self.options.description_selector:
                desc_node = item.select_one(self.options.description_selector)
                description = desc_node.get_text(strip=True) if desc_node else None

            asset_links: list[str] = []
            if self.options.asset_selector:
                for asset_node in item.select(self.options.asset_selector):
                    href = asset_node.get("href")
                    if isinstance(href, str):
                        asset_links.append(href)

            link_href = link_node.get("href")
            source_url = link_href if isinstance(link_href, str) else url
            yield DatasetRecord(
                source_name=ctx.config.source.name,
                source_dataset_id=None,
                source_url=source_url,
                title=title_node.get_text(strip=True),
                description=description,
                assets=[AssetRecord(asset_url=link) for link in asset_links],
                raw={"html": str(item)},
            )
            count += 1
            if self.options.max_items and count >= self.options.max_items:
                break
