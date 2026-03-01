from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import httpx

from qdarchive_seeding.app.config_loader import config_hash
from qdarchive_seeding.app.config_models import PipelineConfig
from qdarchive_seeding.app.manifests import RunManifestWriter
from qdarchive_seeding.app.policies import IncrementalPolicy, RetryPolicy
from qdarchive_seeding.app.progress import ProgressBus
from qdarchive_seeding.core.constants import DEFAULT_CHUNK_SIZE_BYTES
from qdarchive_seeding.core.interfaces import AuthProvider, Extractor, Policy, Sink, Transform
from qdarchive_seeding.infra.http.auth import ApiKeyAuth, BearerAuth, NoAuth, OAuth2ClientCredentials
from qdarchive_seeding.infra.http.client import HttpClientSettings, HttpxClient
from qdarchive_seeding.infra.http.rate_limit import RateLimiter
from qdarchive_seeding.infra.extractors.generic_rest import GenericRestExtractor, GenericRestOptions
from qdarchive_seeding.infra.extractors.html_scraper import HtmlScraperExtractor, HtmlScraperOptions
from qdarchive_seeding.infra.extractors.static_list import StaticListExtractor, StaticListOptions
from qdarchive_seeding.infra.extractors.zenodo import ZenodoExtractor, ZenodoOptions
from qdarchive_seeding.infra.logging.logger import LoggerBundle, configure_logger
from qdarchive_seeding.infra.sinks.csv_sink import CSVSink
from qdarchive_seeding.infra.sinks.excel_sink import ExcelSink
from qdarchive_seeding.infra.sinks.mongodb import MongoDBSink
from qdarchive_seeding.infra.sinks.mysql import MySQLSink
from qdarchive_seeding.infra.sinks.sqlite import SQLiteSink
from qdarchive_seeding.infra.storage.checksums import ChecksumComputer
from qdarchive_seeding.infra.storage.downloader import Downloader
from qdarchive_seeding.infra.storage.filesystem import FileSystem
from qdarchive_seeding.infra.storage.paths import PathStrategy
from qdarchive_seeding.infra.transforms.base import TransformChain
from qdarchive_seeding.infra.transforms.deduplicate_assets import DeduplicateAssets
from qdarchive_seeding.infra.transforms.infer_filetypes import InferFileTypes
from qdarchive_seeding.infra.transforms.normalize_fields import NormalizeFields
from qdarchive_seeding.infra.transforms.slugify_dataset import SlugifyDataset
from qdarchive_seeding.infra.transforms.validate_required import ValidateRequiredFields


@dataclass(slots=True)
class Container:
    config: PipelineConfig
    run_id: str
    logger_bundle: LoggerBundle
    auth: AuthProvider
    http_client: HttpxClient
    rate_limiter: RateLimiter
    extractor: Extractor
    transform_chain: TransformChain
    downloader: Downloader
    sink: Sink
    policy: Policy
    progress_bus: ProgressBus
    manifests: RunManifestWriter
    path_strategy: PathStrategy
    filesystem: FileSystem
    config_hash: str


def build_container(
    config: PipelineConfig,
    *,
    run_id: str | None = None,
    force: bool = False,
    retry_failed: bool = False,
    runs_dir: Path = Path("runs"),
    enable_log_queue: bool = False,
) -> Container:
    run_id = run_id or str(uuid4())

    logger_bundle = configure_logger(
        "qdarchive_seeding",
        config.logging,
        run_id=run_id,
        pipeline_id=config.pipeline.id,
        component="runner",
        enable_queue=enable_log_queue,
    )

    auth = _build_auth(config)
    http_client = HttpxClient(HttpClientSettings())
    rate_limiter = RateLimiter(max_per_second=5.0)
    extractor = _build_extractor(config, http_client, auth)
    transform_chain = _build_transforms(config)

    chunk_size = config.storage.chunk_size_bytes or DEFAULT_CHUNK_SIZE_BYTES
    checksum_algo = config.storage.checksum if config.storage.checksum != "none" else "sha256"
    checksum = ChecksumComputer(algo=checksum_algo)
    download_client = httpx.Client(timeout=60.0, headers={"User-Agent": "qdarchive-seeding/0.1"})
    downloader = Downloader(client=download_client, checksum=checksum, chunk_size_bytes=chunk_size)

    sink = _build_sink(config)

    policy: Policy
    if retry_failed:
        policy = RetryPolicy(retry_failed=True)
    else:
        policy = IncrementalPolicy(run_mode=config.pipeline.run_mode, force=force)

    progress_bus = ProgressBus()
    manifests = RunManifestWriter(runs_dir=runs_dir)
    path_strategy = PathStrategy(layout_template=config.storage.layout)
    filesystem = FileSystem(root=Path(config.storage.downloads_root))

    return Container(
        config=config,
        run_id=run_id,
        logger_bundle=logger_bundle,
        auth=auth,
        http_client=http_client,
        rate_limiter=rate_limiter,
        extractor=extractor,
        transform_chain=transform_chain,
        downloader=downloader,
        sink=sink,
        policy=policy,
        progress_bus=progress_bus,
        manifests=manifests,
        path_strategy=path_strategy,
        filesystem=filesystem,
        config_hash=config_hash(config),
    )


def _build_auth(config: PipelineConfig) -> AuthProvider:
    auth_type = config.auth.type
    if auth_type == "none":
        return NoAuth()
    if auth_type == "api_key":
        env_key = config.auth.env.get("api_key", "")
        api_key = os.environ.get(env_key, "")
        placement = config.auth.placement or "header"
        header_name = config.auth.header_name or "X-API-Key"
        return ApiKeyAuth(api_key=api_key, placement=placement, header_name=header_name)
    if auth_type == "bearer":
        env_key = config.auth.env.get("token", config.auth.env.get("api_key", ""))
        token = os.environ.get(env_key, "")
        return BearerAuth(token=token)
    if auth_type == "oauth2":
        token_url = config.auth.env.get("token_url", "")
        client_id_key = config.auth.env.get("client_id", "")
        client_secret_key = config.auth.env.get("client_secret", "")
        scope = config.auth.env.get("scope", "")
        return OAuth2ClientCredentials(
            token_url=os.environ.get(token_url, token_url),
            client_id=os.environ.get(client_id_key, ""),
            client_secret=os.environ.get(client_secret_key, ""),
            scope=os.environ.get(scope, scope),
        )
    return NoAuth()


def _build_extractor(
    config: PipelineConfig, http_client: HttpxClient, auth: AuthProvider
) -> Extractor:
    name = config.extractor.name
    options = config.extractor.options

    if name == "zenodo_extractor":
        return ZenodoExtractor(
            http_client=http_client,
            auth=auth,
            options=ZenodoOptions(
                include_files=options.get("include_files", True),
                max_pages=options.get("max_pages"),
            ),
        )
    if name == "generic_rest_extractor":
        return GenericRestExtractor(
            http_client=http_client,
            auth=auth,
            options=GenericRestOptions(
                records_path=options.get("records_path", "items"),
                max_pages=options.get("max_pages"),
            ),
        )
    if name == "html_scraper_extractor":
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
    if name == "static_list_extractor":
        return StaticListExtractor(
            options=StaticListOptions(records=options.get("records", [])),
        )
    msg = f"Unknown extractor: {name}"
    raise ValueError(msg)


def _build_transforms(config: PipelineConfig) -> TransformChain:
    transforms: list[Transform] = []
    for t in config.transforms:
        transform = _build_single_transform(t.name, t.options)
        if transform is not None:
            transforms.append(transform)
    return TransformChain(transforms=transforms)


def _build_single_transform(name: str, options: dict[str, object]) -> Transform | None:
    if name == "validate_required_fields":
        fields = options.get("required_fields", [])
        if not isinstance(fields, list):
            fields = []
        return ValidateRequiredFields(
            name=name,
            required_fields=[str(f) for f in fields],
        )
    if name == "normalize_fields":
        return NormalizeFields(name=name)
    if name == "infer_filetypes":
        return InferFileTypes(name=name)
    if name == "deduplicate_assets":
        return DeduplicateAssets(name=name)
    if name == "slugify_dataset":
        return SlugifyDataset(name=name)
    return None


def _build_sink(config: PipelineConfig) -> Sink:
    sink_type = config.sink.type
    options = config.sink.options

    if sink_type == "sqlite":
        return SQLiteSink(name="sqlite", path=Path(options.get("path", "./metadata/qdarchive.sqlite")))  # type: ignore[arg-type]
    if sink_type == "csv":
        return CSVSink(
            name="csv",
            dataset_path=Path(options.get("dataset_path", "./metadata/datasets.csv")),  # type: ignore[arg-type]
            asset_path=Path(options.get("asset_path", "./metadata/assets.csv")),  # type: ignore[arg-type]
        )
    if sink_type == "excel":
        return ExcelSink(name="excel", path=Path(options.get("path", "./metadata/qdarchive.xlsx")))  # type: ignore[arg-type]
    if sink_type == "mysql":
        return MySQLSink(
            name="mysql",
            host=str(options.get("host", "localhost")),
            port=int(options.get("port", 3306)),  # type: ignore[arg-type]
            database=str(options.get("database", "qdarchive")),
            user=str(options.get("user", "root")),
            password=os.environ.get(str(options.get("password_env", "")), ""),
        )
    if sink_type == "mongodb":
        return MongoDBSink(
            name="mongodb",
            uri=str(options.get("uri", "mongodb://localhost:27017")),
            database=str(options.get("database", "qdarchive")),
        )
    msg = f"Unknown sink type: {sink_type}"
    raise ValueError(msg)
