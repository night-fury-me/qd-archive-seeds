from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.core.interfaces import AuthProvider, HttpClient, RunContext
from qdarchive_seeding.infra.http.pagination import OffsetPagination

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class SyracuseQdrOptions:
    max_datasets: int | None = None
    _safety_max_datasets: int = 500


@dataclass(slots=True)
class SyracuseQdrExtractor:
    """Two-phase extractor for Syracuse QDR (Dataverse).

    Phase 1: search all datasets via ``/search?q=*&type=dataset``.
    Phase 2: for each dataset, fetch the full file listing via
             ``/datasets/:persistentId/?persistentId={doi}``.
    """

    http_client: HttpClient
    auth: AuthProvider
    options: SyracuseQdrOptions

    async def extract(self, ctx: RunContext) -> AsyncIterator[DatasetRecord]:
        base_url = ctx.config.source.base_url.rstrip("/")
        search_endpoint = ctx.config.source.endpoints.get("search", "/search")
        dataset_endpoint = ctx.config.source.endpoints.get("dataset", "/datasets/:persistentId/")
        search_url = f"{base_url}{search_endpoint}"

        headers: dict[str, str] = {}
        params = dict(ctx.config.source.params)
        headers, params = self.auth.apply(headers, params)

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
        datasets_inspected = 0

        for page_params in paginator.iter_params(params):
            if datasets_inspected >= effective_max:
                logger.warning("Reached max datasets limit (%d), stopping", effective_max)
                break

            response = await self.http_client.get(
                search_url, headers=headers, params=page_params, timeout=90.0
            )
            payload = response.json()

            data = payload.get("data", {})
            items = data.get("items", [])
            total_count = data.get("total_count", 0)

            if not isinstance(items, list) or not items:
                break

            logger.info(
                "Search page: fetched %d datasets (%d/%d total)",
                len(items),
                datasets_inspected + len(items),
                total_count,
            )

            for item in items:
                if datasets_inspected >= effective_max:
                    break
                datasets_inspected += 1

                global_id = item.get("global_id", "")
                if not global_id:
                    logger.debug("Skipping dataset without global_id: %s", item.get("name"))
                    continue

                files = await self._fetch_dataset_files(
                    base_url, dataset_endpoint, global_id, headers
                )
                if files is None:
                    continue

                file_list, license_name = files

                assets = [
                    AssetRecord(
                        asset_url=f"{base_url}/access/datafile/{df['id']}",
                        local_filename=df.get("filename", ""),
                        size_bytes=df.get("filesize"),
                    )
                    for df in file_list
                    if df.get("id") is not None
                ]

                doi = global_id.removeprefix("doi:") if global_id.startswith("doi:") else global_id

                yield DatasetRecord(
                    source_name=ctx.config.source.name,
                    source_dataset_id=global_id,
                    source_url=f"{base_url.rsplit('/api', 1)[0]}"
                    f"/dataset.xhtml?persistentId={global_id}",
                    title=item.get("name"),
                    description=item.get("description"),
                    doi=doi,
                    license=license_name,
                    year=_extract_year(item.get("published_at")),
                    owner_name=_first_author(item.get("authors")),
                    assets=assets,
                    raw=item,
                )

            if datasets_inspected >= total_count:
                logger.info("All %d datasets fetched", total_count)
                break

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
        url = f"{base_url}{dataset_endpoint}"
        params: dict[str, Any] = {"persistentId": persistent_id}

        try:
            response = await self.http_client.get(url, headers=headers, params=params, timeout=90.0)
            payload = response.json()
        except Exception:
            logger.warning("Failed to fetch files for dataset %s", persistent_id, exc_info=True)
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
