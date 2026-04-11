from __future__ import annotations

import html as html_mod
import logging
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlparse

import httpx

from qdarchive_seeding.core.constants import (
    DOWNLOAD_METHOD_API,
    PERSON_ROLE_CREATOR,
)
from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord, PersonRole
from qdarchive_seeding.core.interfaces import AuthProvider, HttpClient, RunContext
from qdarchive_seeding.core.progress import PageProgress, QueryProgress
from qdarchive_seeding.infra.extractors._checkpoint import (
    get_resume_page,
    is_query_done,
    mark_query_done,
)
from qdarchive_seeding.infra.http.auth import apply_auth_async
from qdarchive_seeding.infra.storage.paths import safe_filename

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
    _icpsr_cookie_cache: dict[str, str] = field(default_factory=dict, repr=False)

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

        existing_ids = ctx.existing_dataset_ids
        seen_ids: set[str] = set(existing_ids) if existing_ids else set()
        prefix = strategy.base_query_prefix
        facets = strategy.facet_filters
        bus = ctx.progress_bus
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
                ctx, query, seen_ids=seen_ids, query_string=query, extra_params=facets
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
                ctx, query, seen_ids=seen_ids, query_string=query, extra_params=facets
            ):
                yield record

    async def _extract_single_query(
        self,
        ctx: RunContext,
        query: str,
        *,
        seen_ids: set[str] | None = None,
        query_string: str = "",
        extra_params: dict[str, str] | None = None,
    ) -> AsyncIterator[DatasetRecord]:
        """Run a single paginated query against the Dataverse Search API."""
        base_url = ctx.config.source.base_url.rstrip("/")
        search_endpoint = ctx.config.source.endpoints.get("search", "/search")
        url = f"{base_url}{search_endpoint}"

        headers: dict[str, str] = {}
        params: dict[str, Any] = {"q": query, "type": "dataset"}
        if extra_params:
            params.update(extra_params)
        headers, params = await apply_auth_async(self.auth, headers, params)

        source_cfg = ctx.config.source
        per_page = self.options.per_page
        effective_max_pages = self.options.max_pages or self.options._safety_max_pages
        bus = ctx.progress_bus
        checkpoint = ctx.checkpoint

        if is_query_done(checkpoint, query_string):
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

        resume_from = get_resume_page(checkpoint, query_string)
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
                record = await self._build_record(ctx, item, source_cfg, query_string, seen_ids)
                if record is not None:
                    yield record

            # Advance offset
            start += per_page
            if start >= total_count:
                break

        mark_query_done(checkpoint, query_string)

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

        # Detect harvested datasets: their URL points to a different Dataverse
        dataset_url_raw = item.get("url", "")
        repo_host = urlparse(source_cfg.repository_url).netloc.lower()
        dataset_host = urlparse(dataset_url_raw).netloc.lower()
        is_harvested = bool(dataset_host and dataset_host != repo_host)
        harvested_from = dataset_host if is_harvested else None

        assets: list[AssetRecord] = []
        license_name: str | None = None
        if self.options.include_files:
            if is_harvested:
                # Resolve the actual host (DOI URLs redirect to the real server)
                original_base = await self._resolve_origin_base_url(dataset_url_raw)
                if original_base:
                    resolved_host = urlparse(original_base).netloc.lower()
                    if resolved_host == repo_host:
                        # DOI resolved back to our own Dataverse — treat as native
                        is_harvested = False
                        harvested_from = None
                        logger.info(
                            "Dataset %s resolved back to %s — treating as native",
                            global_id,
                            repo_host,
                        )
                        assets, license_name = await self._fetch_files_and_license(ctx, global_id)
                    else:
                        harvested_from = resolved_host
                        logger.info(
                            "Harvested dataset %s — fetching files from origin %s",
                            global_id,
                            resolved_host,
                        )
                        assets, license_name = await self._fetch_files_and_license(
                            ctx, global_id, base_url_override=original_base
                        )
                        if not assets:
                            logger.info(
                                "Harvested dataset %s — origin %s returned no files "
                                "(may not be a Dataverse installation)",
                                global_id,
                                resolved_host,
                            )
                else:
                    logger.warning(
                        "Harvested dataset %s — could not resolve origin from %s",
                        global_id,
                        dataset_url_raw,
                    )
            else:
                assets, license_name = await self._fetch_files_and_license(ctx, global_id)

        # If no files were found and origin is ICPSR, construct download URLs directly
        if not assets and is_harvested and harvested_from:
            assets = self._build_icpsr_assets(ctx, global_id, harvested_from)

        persons: list[PersonRole] = []
        for author in item.get("authors", []):
            if isinstance(author, str) and author:
                persons.append(PersonRole(name=author, role=PERSON_ROLE_CREATOR))

        # Build project_url as repo + dataset path (not DOI URL)
        dataset_url = item.get("url", "")
        project_url = _build_project_url(source_cfg.repository_url, global_id, dataset_url)
        project_folder = global_id.replace("doi:", "").replace("/", "_")

        # Format DOI as full URL
        doi = _format_doi(global_id) if global_id.startswith("doi:") else None

        # Clean HTML from description
        description = _clean_html(item.get("description"))

        return DatasetRecord(
            source_name=source_cfg.name,
            source_dataset_id=global_id,
            source_url=project_url,
            title=item.get("name"),
            description=description,
            doi=doi,
            license=license_name,
            query_string=query_string,
            repository_id=source_cfg.repository_id,
            repository_url=source_cfg.repository_url,
            language=None,
            upload_date=item.get("published_at"),
            download_method=DOWNLOAD_METHOD_API,
            is_harvested=is_harvested,
            harvested_from=harvested_from,
            download_repository_folder=source_cfg.name,
            download_project_folder=project_folder,
            keywords=item.get("keywords", []),
            persons=persons,
            assets=assets,
            raw=item,
        )

    def _build_icpsr_assets(
        self,
        ctx: RunContext,
        global_id: str,
        harvested_from: str,
    ) -> list[AssetRecord]:
        """Build download assets for ICPSR datasets (Classic or Open)."""
        from qdarchive_seeding.infra.extractors.icpsr_utils import (
            ICPSR_CLASSIC_HOST,
            ICPSR_OPEN_HOST,
            build_classic_icpsr_download_url,
            build_open_icpsr_download_url,
            parse_icpsr_doi,
        )

        if harvested_from not in (ICPSR_CLASSIC_HOST, ICPSR_OPEN_HOST):
            return []

        info = parse_icpsr_doi(global_id)
        if info is None:
            return []

        if info.icpsr_type == "classic":
            logger.info(
                "Classic ICPSR %s — requires manual download (zipcart2 form flow)",
                global_id,
            )
            return [
                AssetRecord(
                    asset_url=build_classic_icpsr_download_url(info.study_id),
                    asset_type="zip_bundle",
                    local_filename=f"ICPSR{info.study_id}.zip",
                    download_status="SKIPPED",
                    metadata={"icpsr_type": "classic", "requires_manual": True},
                )
            ]

        # Open ICPSR — can download with browser session cookies
        download_url = build_open_icpsr_download_url(info.study_id, info.version)
        cookie_header = self._get_icpsr_cookies(ctx, ICPSR_OPEN_HOST)

        metadata: dict[str, Any] = {"icpsr_type": "open"}
        if cookie_header:
            metadata["auth_headers"] = {"Cookie": cookie_header}
        else:
            logger.warning(
                "No browser cookies for %s — Open ICPSR downloads will likely fail. "
                "Log into ICPSR in your browser first.",
                ICPSR_OPEN_HOST,
            )

        logger.info("Open ICPSR %s — download URL constructed", global_id)
        return [
            AssetRecord(
                asset_url=download_url,
                asset_type="zip_bundle",
                local_filename=f"openicpsr_{info.study_id}.zip",
                metadata=metadata,
            )
        ]

    def _get_icpsr_cookies(self, ctx: RunContext, domain: str) -> str:
        """Lazily extract and cache browser cookies for ICPSR domains."""
        if domain in self._icpsr_cookie_cache:
            return self._icpsr_cookie_cache[domain]

        ext_auth = ctx.config.external_auth.get(domain)
        if ext_auth is None or ext_auth.type != "browser_session":
            self._icpsr_cookie_cache[domain] = ""
            return ""

        from qdarchive_seeding.infra.http.browser_cookies import BrowserCookieExtractor

        extractor = BrowserCookieExtractor(browser=ext_auth.browser)  # type: ignore[arg-type]
        cookie_header = extractor.get_cookie_header(domain)
        self._icpsr_cookie_cache[domain] = cookie_header
        return cookie_header

    async def _resolve_origin_base_url(self, dataset_url: str) -> str | None:
        """Resolve a DOI or redirect URL to the actual Dataverse API base URL.

        Follows redirects on the dataset URL (e.g. https://doi.org/10.5683/...)
        to discover the real Dataverse host (e.g. https://borealisdata.ca),
        then returns its API base URL (e.g. https://borealisdata.ca/api).
        Returns None if resolution fails.
        """
        parsed = urlparse(dataset_url)
        # If it's already a Dataverse host (not doi.org), use it directly
        if parsed.netloc.lower() != "doi.org":
            return f"{parsed.scheme}://{parsed.netloc}/api"

        try:
            # Follow redirects via GET; we only need the final URL
            response = await self.http_client.get(dataset_url, headers={}, params={})
            final_url = str(response.url) if hasattr(response, "url") else ""
        except httpx.HTTPStatusError as exc:
            # The target page may return 4xx/5xx (e.g. QDR returns 405 on /citation)
            # but the redirect chain already resolved — extract the URL from the response
            final_url = str(exc.response.url) if hasattr(exc.response, "url") else ""
            logger.debug("DOI %s resolved with status %d", dataset_url, exc.response.status_code)
        except Exception as exc:
            logger.debug("Failed to resolve DOI %s: %s", dataset_url, exc)
            return None

        if final_url:
            final_parsed = urlparse(final_url)
            resolved = f"{final_parsed.scheme}://{final_parsed.netloc}/api"
            logger.debug("Resolved DOI %s → %s", dataset_url, resolved)
            return resolved

        return None

    async def _fetch_files_and_license(
        self,
        ctx: RunContext,
        persistent_id: str,
        *,
        base_url_override: str | None = None,
    ) -> tuple[list[AssetRecord], str | None]:
        """Fetch the file listing and license for a dataset via the Dataverse API.

        Uses the full dataset endpoint (/datasets/:persistentId/) to get
        both the file listing and license information in one call.

        Args:
            base_url_override: When set, fetch from this API base URL
                instead of the configured source. Used for harvested datasets
                so we hit the original Dataverse that actually hosts the files.

        Returns:
            A tuple of (assets, license_name).
        """
        base_url = (base_url_override or ctx.config.source.base_url).rstrip("/")

        headers: dict[str, str] = {}
        params: dict[str, Any] = {"persistentId": persistent_id}
        origin_auth_headers: dict[str, str] = {}
        if base_url_override is None:
            headers, params = await apply_auth_async(self.auth, headers, params)
            native_headers, _ = await apply_auth_async(self.auth, {}, {})
            origin_auth_headers = native_headers
        else:
            origin_host = urlparse(base_url_override).netloc.lower()
            ext_auth = ctx.config.external_auth.get(origin_host)
            if ext_auth:
                import os

                token = os.environ.get(ext_auth.env.get("api_key", ""), "")
                if token:
                    headers[ext_auth.header_name] = token
                    origin_auth_headers[ext_auth.header_name] = token
                    logger.debug("Applied external auth for %s", origin_host)

        # Use the full dataset endpoint to get both files and license
        dataset_url = f"{base_url}/datasets/:persistentId/"
        license_name: str | None = None
        try:
            response = await self.http_client.get(dataset_url, headers=headers, params=params)
            payload = response.json()
            data = payload.get("data", {})
            latest = data.get("latestVersion", {})

            # Extract license
            license_info = latest.get("license", {})
            if isinstance(license_info, dict):
                license_name = license_info.get("name")
            elif isinstance(license_info, str):
                license_name = license_info

            # Extract files from the dataset response
            raw_files = latest.get("files", [])
        except Exception as exc:
            logger.debug(
                "Failed to fetch dataset %s from %s: %s — falling back to files endpoint",
                persistent_id,
                base_url,
                exc,
            )
            # Fallback to the files-only endpoint
            fallback = await self._fetch_files(
                ctx, persistent_id, base_url_override=base_url_override
            )
            return fallback, None

        assets: list[AssetRecord] = []
        for file_entry in raw_files:
            data_file = file_entry.get("dataFile", {})
            raw_filename = data_file.get("filename", "")
            file_id = data_file.get("id")
            if not raw_filename or not file_id:
                continue

            filename = safe_filename(raw_filename)
            download_url = f"{base_url}/access/datafile/{file_id}"
            file_type = PurePosixPath(filename).suffix.lstrip(".") if filename else None

            metadata: dict[str, Any] = {}
            if origin_auth_headers:
                metadata["auth_headers"] = origin_auth_headers

            assets.append(
                AssetRecord(
                    asset_url=download_url,
                    local_filename=filename,
                    file_type=file_type,
                    size_bytes=data_file.get("filesize"),
                    metadata=metadata or None,
                )
            )

        return assets, license_name

    async def _fetch_files(
        self,
        ctx: RunContext,
        persistent_id: str,
        *,
        base_url_override: str | None = None,
    ) -> list[AssetRecord]:
        """Fetch the file listing for a dataset via the Dataverse Files API.

        Args:
            base_url_override: When set, fetch files from this API base URL
                instead of the configured source. Used for harvested datasets
                so we hit the original Dataverse that actually hosts the files.
        """
        base_url = (base_url_override or ctx.config.source.base_url).rstrip("/")
        files_endpoint = ctx.config.source.endpoints.get(
            "files", "/datasets/:persistentId/versions/:latest/files"
        )
        url = f"{base_url}{files_endpoint}"

        headers: dict[str, str] = {}
        params: dict[str, Any] = {"persistentId": persistent_id}
        origin_auth_headers: dict[str, str] = {}
        if base_url_override is None:
            headers, params = await apply_auth_async(self.auth, headers, params)
            native_headers, _ = await apply_auth_async(self.auth, {}, {})
            origin_auth_headers = native_headers
        else:
            origin_host = urlparse(base_url_override).netloc.lower()
            ext_auth = ctx.config.external_auth.get(origin_host)
            if ext_auth:
                import os

                token = os.environ.get(ext_auth.env.get("api_key", ""), "")
                if token:
                    headers[ext_auth.header_name] = token
                    origin_auth_headers[ext_auth.header_name] = token
                    logger.debug("Applied external auth for %s", origin_host)

        try:
            response = await self.http_client.get(url, headers=headers, params=params)
            payload = response.json()
        except Exception as exc:
            logger.debug("Failed to fetch files for %s from %s: %s", persistent_id, base_url, exc)
            return []

        data = payload.get("data", [])
        if not isinstance(data, list):
            return []

        assets: list[AssetRecord] = []
        for file_entry in data:
            data_file = file_entry.get("dataFile", {})
            raw_filename = data_file.get("filename", "")
            file_id = data_file.get("id")
            if not raw_filename or not file_id:
                continue

            filename = safe_filename(raw_filename)
            download_url = f"{base_url}/access/datafile/{file_id}"
            file_type = PurePosixPath(filename).suffix.lstrip(".") if filename else None

            metadata: dict[str, Any] = {}
            if origin_auth_headers:
                metadata["auth_headers"] = origin_auth_headers

            assets.append(
                AssetRecord(
                    asset_url=download_url,
                    local_filename=filename,
                    file_type=file_type,
                    size_bytes=data_file.get("filesize"),
                    metadata=metadata or None,
                )
            )

        return assets


def _format_doi(global_id: str) -> str:
    """Convert a Dataverse global_id like ``doi:10.7910/DVN/XXX`` to a full DOI URL."""
    bare = global_id.removeprefix("doi:")
    return f"https://doi.org/{bare}"


def _build_project_url(repository_url: str | None, global_id: str, raw_url: str) -> str:
    """Build a project URL using the repository domain instead of a DOI redirect.

    Produces URLs like ``https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/...``
    """
    if repository_url and global_id.startswith("doi:"):
        repo = repository_url.rstrip("/")
        return f"{repo}/dataset.xhtml?persistentId={global_id}"
    return raw_url


def _clean_html(text: str | None) -> str | None:
    """Strip HTML tags and decode HTML entities."""
    if text is None:
        return None
    clean = html_mod.unescape(text)
    clean = re.sub(r"<[^>]+>", "", clean)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean or None
