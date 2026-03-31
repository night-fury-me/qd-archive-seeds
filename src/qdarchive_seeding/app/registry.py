from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from qdarchive_seeding.core.exceptions import RegistryError
from qdarchive_seeding.core.interfaces import (
    AuthProvider,
    ComponentSpec,
    Extractor,
    Registry,
    Sink,
    Transform,
)


@dataclass(slots=True)
class RegistryEntry:
    component: Any
    spec: ComponentSpec | None = None


class InMemoryRegistry(Registry):
    def __init__(self) -> None:
        self._items: dict[str, RegistryEntry] = {}

    def register(self, name: str, component: Any, spec: ComponentSpec | None = None) -> None:
        if name in self._items:
            raise RegistryError(f"Component already registered: {name}")
        self._items[name] = RegistryEntry(component=component, spec=spec)

    def get(self, name: str) -> Any:
        try:
            return self._items[name].component
        except KeyError as exc:
            raise RegistryError(f"Component not found: {name}") from exc

    def spec(self, name: str) -> ComponentSpec | None:
        try:
            return self._items[name].spec
        except KeyError as exc:
            raise RegistryError(f"Component not found: {name}") from exc

    def list(self) -> list[str]:
        return sorted(self._items.keys())


# ---------------------------------------------------------------------------
# Factory type aliases
# ---------------------------------------------------------------------------
# Auth factory: (config) -> AuthProvider
#   config is PipelineConfig but we use Any to avoid circular imports at module level.
AuthFactory = Callable[..., AuthProvider]
# Extractor factory: (http_client, auth, options) -> Extractor
ExtractorFactory = Callable[..., Extractor]
# Transform factory: (name, options) -> Transform
TransformFactory = Callable[..., Transform]
# Sink factory: (options) -> Sink
SinkFactory = Callable[..., Sink]


@dataclass(slots=True)
class ComponentRegistries:
    """Holds per-category registries for all component types."""

    auth: InMemoryRegistry = field(default_factory=InMemoryRegistry)
    extractors: InMemoryRegistry = field(default_factory=InMemoryRegistry)
    transforms: InMemoryRegistry = field(default_factory=InMemoryRegistry)
    sinks: InMemoryRegistry = field(default_factory=InMemoryRegistry)
    policies: InMemoryRegistry = field(default_factory=InMemoryRegistry)
    checksums: InMemoryRegistry = field(default_factory=InMemoryRegistry)
    downloaders: InMemoryRegistry = field(default_factory=InMemoryRegistry)
    progress_buses: InMemoryRegistry = field(default_factory=InMemoryRegistry)
    manifests: InMemoryRegistry = field(default_factory=InMemoryRegistry)
    path_strategies: InMemoryRegistry = field(default_factory=InMemoryRegistry)
    filesystems: InMemoryRegistry = field(default_factory=InMemoryRegistry)


# ---------------------------------------------------------------------------
# Auth factories
# ---------------------------------------------------------------------------


def _build_no_auth(config: Any) -> AuthProvider:
    from qdarchive_seeding.infra.http.auth import NoAuth

    return NoAuth()


def _build_api_key_auth(config: Any) -> AuthProvider:
    from qdarchive_seeding.infra.http.auth import ApiKeyAuth

    env_key = config.auth.env.get("api_key", "")
    api_key = os.environ.get(env_key, "")
    placement = config.auth.placement or "header"
    header_name = config.auth.header_name or "X-API-Key"
    return ApiKeyAuth(api_key=api_key, placement=placement, header_name=header_name)


def _build_bearer_auth(config: Any) -> AuthProvider:
    from qdarchive_seeding.infra.http.auth import BearerAuth

    env_key = config.auth.env.get("token", config.auth.env.get("api_key", ""))
    token = os.environ.get(env_key, "")
    return BearerAuth(token=token)


def _build_oauth2_auth(config: Any) -> AuthProvider:
    from qdarchive_seeding.infra.http.auth import OAuth2ClientCredentials

    token_url_key = config.auth.env.get("token_url", "")
    client_id_key = config.auth.env.get("client_id", "")
    client_secret_key = config.auth.env.get("client_secret", "")
    scope = config.auth.env.get("scope", "")
    token_url = os.environ.get(token_url_key, token_url_key)
    if "://" not in token_url:
        msg = f"OAuth2 token_url env var '{token_url_key}' is not set or is not a valid URL"
        raise ValueError(msg)
    return OAuth2ClientCredentials(
        token_url=token_url,
        client_id=os.environ.get(client_id_key, ""),
        client_secret=os.environ.get(client_secret_key, ""),
        scope=os.environ.get(scope, scope),
    )


# ---------------------------------------------------------------------------
# Extractor factories
# ---------------------------------------------------------------------------


def _build_zenodo_extractor(
    http_client: Any,
    auth: AuthProvider,
    options: dict[str, Any],
) -> Extractor:
    from qdarchive_seeding.infra.extractors.zenodo import ZenodoExtractor, ZenodoOptions

    return ZenodoExtractor(
        http_client=http_client,
        auth=auth,
        options=ZenodoOptions(
            include_files=options.get("include_files", True),
            max_pages=options.get("max_pages"),
            ext_batch_size=options.get("ext_batch_size", 10),
            nl_batch_size=options.get("nl_batch_size", 4),
            auto_date_split=options.get("auto_date_split", True),
        ),
    )


def _build_generic_rest_extractor(
    http_client: Any,
    auth: AuthProvider,
    options: dict[str, Any],
) -> Extractor:
    from qdarchive_seeding.infra.extractors.generic_rest import (
        GenericRestExtractor,
        GenericRestOptions,
    )

    return GenericRestExtractor(
        http_client=http_client,
        auth=auth,
        options=GenericRestOptions(
            records_path=options.get("records_path", "items"),
            max_pages=options.get("max_pages"),
        ),
    )


def _build_html_scraper_extractor(
    http_client: Any,
    auth: AuthProvider,
    options: dict[str, Any],
) -> Extractor:
    from qdarchive_seeding.infra.extractors.html_scraper import (
        HtmlScraperExtractor,
        HtmlScraperOptions,
    )

    return HtmlScraperExtractor(
        http_client=http_client,
        options=HtmlScraperOptions(
            list_selector=options.get("list_selector", ""),
            title_selector=options.get("title_selector", ""),
            link_selector=options.get("link_selector", ""),
            description_selector=options.get("description_selector"),
            asset_selector=options.get("asset_selector"),
            max_items=options.get("max_items"),
        ),
    )


def _build_syracuse_qdr_extractor(
    http_client: Any,
    auth: AuthProvider,
    options: dict[str, Any],
) -> Extractor:
    from qdarchive_seeding.infra.extractors.syracuse_qdr import (
        SyracuseQdrExtractor,
        SyracuseQdrOptions,
    )

    return SyracuseQdrExtractor(
        http_client=http_client,
        auth=auth,
        options=SyracuseQdrOptions(
            max_datasets=options.get("max_datasets"),
            include_files=options.get("include_files", True),
        ),
    )


def _build_harvard_dataverse_extractor(
    http_client: Any,
    auth: AuthProvider,
    options: dict[str, Any],
) -> Extractor:
    from qdarchive_seeding.infra.extractors.harvard_dataverse import (
        HarvardDataverseExtractor,
        HarvardDataverseOptions,
    )

    return HarvardDataverseExtractor(
        http_client=http_client,
        auth=auth,
        options=HarvardDataverseOptions(
            include_files=options.get("include_files", True),
            max_pages=options.get("max_pages"),
            per_page=options.get("per_page", 10),
        ),
    )


def _build_static_list_extractor(
    http_client: Any,
    auth: AuthProvider,
    options: dict[str, Any],
) -> Extractor:
    from qdarchive_seeding.infra.extractors.static_list import (
        StaticListExtractor,
        StaticListOptions,
    )

    return StaticListExtractor(
        options=StaticListOptions(records=options.get("records", [])),
    )


# ---------------------------------------------------------------------------
# Transform factories
# ---------------------------------------------------------------------------


def _build_validate_required_fields(name: str, options: dict[str, object]) -> Transform:
    from qdarchive_seeding.infra.transforms.validate_required import ValidateRequiredFields

    fields = options.get("required_fields", [])
    if not isinstance(fields, list):
        fields = []
    return ValidateRequiredFields(name=name, required_fields=[str(f) for f in fields])


def _build_normalize_fields(name: str, options: dict[str, object]) -> Transform:
    from qdarchive_seeding.infra.transforms.normalize_fields import NormalizeFields

    return NormalizeFields(name=name)


def _build_infer_filetypes(name: str, options: dict[str, object]) -> Transform:
    from qdarchive_seeding.infra.transforms.infer_filetypes import InferFileTypes

    return InferFileTypes(name=name)


def _build_deduplicate_assets(name: str, options: dict[str, object]) -> Transform:
    from qdarchive_seeding.infra.transforms.deduplicate_assets import DeduplicateAssets

    return DeduplicateAssets(name=name)


def _build_slugify_dataset(name: str, options: dict[str, object]) -> Transform:
    from qdarchive_seeding.infra.transforms.slugify_dataset import SlugifyDataset

    return SlugifyDataset(name=name)


def _build_classify_qda_files(name: str, options: dict[str, object]) -> Transform:
    from qdarchive_seeding.infra.transforms.classify_qda_files import ClassifyQdaFiles

    return ClassifyQdaFiles(name=name)


def _build_filter_by_extensions(name: str, options: dict[str, object]) -> Transform:
    from qdarchive_seeding.infra.transforms.filter_by_extensions import FilterByExtensions

    categories = options.get("categories", ["analysis_data"])
    if not isinstance(categories, list):
        categories = ["analysis_data"]
    extra = options.get("extra_extensions", [])
    if not isinstance(extra, list):
        extra = []
    return FilterByExtensions(
        name=name,
        categories=[str(c) for c in categories],
        extra_extensions=[str(e) for e in extra],
    )


# ---------------------------------------------------------------------------
# Sink factories
# ---------------------------------------------------------------------------


def _build_sqlite_sink(options: dict[str, Any]) -> Sink:
    from qdarchive_seeding.infra.sinks.sqlite import SQLiteSink

    return SQLiteSink(
        name="sqlite",
        path=Path(str(options.get("path", "./metadata/qdarchive.sqlite"))),
    )


def _build_csv_sink(options: dict[str, Any]) -> Sink:
    from qdarchive_seeding.infra.sinks.csv_sink import CSVSink

    return CSVSink(
        name="csv",
        dataset_path=Path(str(options.get("dataset_path", "./metadata/datasets.csv"))),
        asset_path=Path(str(options.get("asset_path", "./metadata/assets.csv"))),
    )


def _build_excel_sink(options: dict[str, Any]) -> Sink:
    from qdarchive_seeding.infra.sinks.excel_sink import ExcelSink

    return ExcelSink(
        name="excel",
        path=Path(str(options.get("path", "./metadata/qdarchive.xlsx"))),
    )


def _build_mysql_sink(options: dict[str, Any]) -> Sink:
    from qdarchive_seeding.infra.sinks.mysql import MySQLSink

    port_val = options.get("port", 3306)
    return MySQLSink(
        name="mysql",
        host=str(options.get("host", "localhost")),
        port=int(port_val) if port_val is not None else 3306,
        database=str(options.get("database", "qdarchive")),
        user=str(options.get("user", "root")),
        password=os.environ.get(str(options.get("password_env", "")), ""),
    )


def _build_mongodb_sink(options: dict[str, Any]) -> Sink:
    from qdarchive_seeding.infra.sinks.mongodb import MongoDBSink

    return MongoDBSink(
        name="mongodb",
        uri=str(options.get("uri", "mongodb://localhost:27017")),
        database=str(options.get("database", "qdarchive")),
    )


# ---------------------------------------------------------------------------
# Policy factories
# ---------------------------------------------------------------------------


def _build_incremental_policy(run_mode: str, fresh_download: bool) -> Any:
    from qdarchive_seeding.app.policies import IncrementalPolicy

    return IncrementalPolicy(run_mode=run_mode, fresh_download=fresh_download)


def _build_retry_policy(retry_failed: bool) -> Any:
    from qdarchive_seeding.app.policies import RetryPolicy

    return RetryPolicy(retry_failed=retry_failed)


# ---------------------------------------------------------------------------
# Checksum factories
# ---------------------------------------------------------------------------


def _build_checksum_computer(algo: str) -> Any:
    from qdarchive_seeding.infra.storage.checksums import ChecksumComputer

    return ChecksumComputer(algo=algo)


# ---------------------------------------------------------------------------
# Downloader factories
# ---------------------------------------------------------------------------


def _build_downloader(client: Any, checksum: Any, chunk_size_bytes: int) -> Any:
    from qdarchive_seeding.infra.storage.downloader import Downloader

    return Downloader(client=client, checksum=checksum, chunk_size_bytes=chunk_size_bytes)


# ---------------------------------------------------------------------------
# ProgressBus factories
# ---------------------------------------------------------------------------


def _build_progress_bus() -> Any:
    from qdarchive_seeding.app.progress import ProgressBus

    return ProgressBus()


# ---------------------------------------------------------------------------
# Manifest factories
# ---------------------------------------------------------------------------


def _build_run_manifest_writer(runs_dir: Path) -> Any:
    from qdarchive_seeding.app.manifests import RunManifestWriter

    return RunManifestWriter(runs_dir=runs_dir)


# ---------------------------------------------------------------------------
# PathStrategy factories
# ---------------------------------------------------------------------------


def _build_path_strategy(layout_template: str) -> Any:
    from qdarchive_seeding.infra.storage.paths import PathStrategy

    return PathStrategy(layout_template=layout_template)


# ---------------------------------------------------------------------------
# FileSystem factories
# ---------------------------------------------------------------------------


def _build_filesystem(root: Path) -> Any:
    from qdarchive_seeding.infra.storage.filesystem import FileSystem

    return FileSystem(root=root)


# ---------------------------------------------------------------------------
# Default registry population
# ---------------------------------------------------------------------------


def create_default_registries() -> ComponentRegistries:
    """Create and populate registries with all built-in component factories."""
    registries = ComponentRegistries()

    # Auth providers
    registries.auth.register("none", _build_no_auth)
    registries.auth.register("api_key", _build_api_key_auth)
    registries.auth.register("bearer", _build_bearer_auth)
    registries.auth.register("oauth2", _build_oauth2_auth)

    # Extractors
    registries.extractors.register("zenodo_extractor", _build_zenodo_extractor)
    registries.extractors.register("generic_rest_extractor", _build_generic_rest_extractor)
    registries.extractors.register("html_scraper_extractor", _build_html_scraper_extractor)
    registries.extractors.register("syracuse_qdr_extractor", _build_syracuse_qdr_extractor)
    registries.extractors.register("static_list_extractor", _build_static_list_extractor)
    registries.extractors.register(
        "harvard_dataverse_extractor", _build_harvard_dataverse_extractor
    )

    # Transforms
    registries.transforms.register("validate_required_fields", _build_validate_required_fields)
    registries.transforms.register("normalize_fields", _build_normalize_fields)
    registries.transforms.register("infer_filetypes", _build_infer_filetypes)
    registries.transforms.register("deduplicate_assets", _build_deduplicate_assets)
    registries.transforms.register("slugify_dataset", _build_slugify_dataset)
    registries.transforms.register("classify_qda_files", _build_classify_qda_files)
    registries.transforms.register("filter_by_extensions", _build_filter_by_extensions)

    # Sinks
    registries.sinks.register("sqlite", _build_sqlite_sink)
    registries.sinks.register("csv", _build_csv_sink)
    registries.sinks.register("excel", _build_excel_sink)
    registries.sinks.register("mysql", _build_mysql_sink)
    registries.sinks.register("mongodb", _build_mongodb_sink)

    # Policies
    registries.policies.register("incremental", _build_incremental_policy)
    registries.policies.register("retry", _build_retry_policy)

    # Checksums
    registries.checksums.register("default", _build_checksum_computer)

    # Downloaders
    registries.downloaders.register("default", _build_downloader)

    # Progress buses
    registries.progress_buses.register("default", _build_progress_bus)

    # Manifests
    registries.manifests.register("default", _build_run_manifest_writer)

    # Path strategies
    registries.path_strategies.register("default", _build_path_strategy)

    # Filesystems
    registries.filesystems.register("default", _build_filesystem)

    return registries
