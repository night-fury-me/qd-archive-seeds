from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse
from uuid import uuid4

import httpx

from qdarchive_seeding.app.checkpoint import CheckpointManager
from qdarchive_seeding.app.config_loader import config_hash
from qdarchive_seeding.app.config_models import PipelineConfig, TransformSettings
from qdarchive_seeding.app.registry import ComponentRegistries, create_default_registries
from qdarchive_seeding.core.constants import DEFAULT_CHUNK_SIZE_BYTES
from qdarchive_seeding.core.interfaces import AuthProvider, Extractor, Policy, Sink, Transform
from qdarchive_seeding.infra.http.client import HttpClientSettings, HttpxClient
from qdarchive_seeding.infra.http.rate_limit import RateLimiter
from qdarchive_seeding.infra.logging.logger import LoggerBundle, configure_logger
from qdarchive_seeding.infra.transforms.base import TransformChain

if TYPE_CHECKING:
    from qdarchive_seeding.app.manifests import RunManifestWriter
    from qdarchive_seeding.app.progress import ProgressBus
    from qdarchive_seeding.infra.storage.downloader import Downloader
    from qdarchive_seeding.infra.storage.filesystem import FileSystem
    from qdarchive_seeding.infra.storage.paths import PathStrategy


@dataclass(slots=True)
class Container:
    config: PipelineConfig
    run_id: str
    logger_bundle: LoggerBundle
    auth: AuthProvider
    http_client: HttpxClient
    rate_limiter: RateLimiter
    extractor: Extractor
    pre_transform_chain: TransformChain
    post_transform_chain: TransformChain
    downloader: Downloader
    sink: Sink
    policy: Policy
    progress_bus: ProgressBus
    manifests: RunManifestWriter
    path_strategy: PathStrategy
    filesystem: FileSystem
    config_hash: str
    checkpoint: CheckpointManager

    async def close(self) -> None:
        """Close async clients to prevent resource leaks."""
        await self.http_client.close()
        await self.downloader.close()


def _load_dotenv(path: Path = Path(".env")) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        # Strip optional 'export ' prefix
        if line.startswith("export "):
            line = line[7:]

        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue

        # Strip inline comments (only outside quotes)
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            # Quoted value — strip the quotes but preserve inner content
            cleaned_value = value[1:-1]
        else:
            # Unquoted — strip inline comments
            comment_idx = value.find(" #")
            if comment_idx != -1:
                value = value[:comment_idx]
            cleaned_value = value.strip()

        os.environ.setdefault(key, cleaned_value)


def build_container(
    config: PipelineConfig,
    *,
    run_id: str | None = None,
    fresh_download: bool = False,
    retry_failed: bool = False,
    runs_dir: Path = Path("runs"),
    enable_log_queue: bool = False,
    registries: ComponentRegistries | None = None,
) -> Container:
    _load_dotenv()
    run_id = run_id or str(uuid4())

    if registries is None:
        registries = create_default_registries()

    logger_bundle = configure_logger(
        "qdarchive_seeding",
        config.logging,
        run_id=run_id,
        pipeline_id=config.pipeline.id,
        component="runner",
        enable_queue=enable_log_queue,
    )

    auth = _build_auth(config, registries)
    http_settings = HttpClientSettings(
        timeout_seconds=config.http.timeout_seconds,
        max_retries=config.http.max_retries,
        backoff_min=config.http.backoff_min,
        backoff_max=config.http.backoff_max,
    )
    rate_limiter = RateLimiter(max_per_second=config.http.rate_limit_per_second)
    http_client = HttpxClient(http_settings, rate_limiter=rate_limiter)
    extractor = _build_extractor(config, http_client, auth, registries)
    pre_transform_chain = _build_transforms(config.pre_transforms, registries)
    post_transform_chain = _build_transforms(config.post_transforms, registries)

    chunk_size = config.storage.chunk_size_bytes or DEFAULT_CHUNK_SIZE_BYTES
    checksum_factory = registries.checksums.get("default")
    checksum = checksum_factory(config.storage.checksum)
    download_headers: dict[str, str] = {
        "User-Agent": "qdarchive-seeding/0.1",
    }
    download_client = httpx.AsyncClient(
        timeout=120.0,
        headers=download_headers,
        follow_redirects=True,
    )

    # Build auth resolver: maps asset URL domain → auth headers at download time.
    # This works for both in-memory and DB-loaded assets (no metadata needed).
    primary_auth_headers, _ = auth.apply({}, {})
    primary_host = urlparse(config.source.base_url).netloc.lower()
    host_auth_map: dict[str, dict[str, str]] = {}
    if primary_auth_headers and primary_host:
        host_auth_map[primary_host] = primary_auth_headers
    for host, ext_auth in config.external_auth.items():
        if ext_auth.type == "api_key":
            token = os.environ.get(ext_auth.env.get("api_key", ""), "")
            if token:
                host_auth_map[host.lower()] = {ext_auth.header_name: token}
        elif ext_auth.type == "browser_session":
            from qdarchive_seeding.infra.http.browser_cookies import BrowserCookieExtractor

            cookie_extractor = BrowserCookieExtractor(browser=ext_auth.browser)  # type: ignore[arg-type]
            cookie_header = cookie_extractor.get_cookie_header(host)
            if cookie_header:
                host_auth_map[host.lower()] = {"Cookie": cookie_header}

    def _resolve_auth(asset_url: str) -> dict[str, str]:
        host = urlparse(asset_url).netloc.lower()
        return host_auth_map.get(host, {})

    downloader_factory = registries.downloaders.get("default")
    downloader = downloader_factory(download_client, checksum, chunk_size)
    downloader.auth_resolver = _resolve_auth

    sink = _build_sink(config, registries)
    max_ds_bytes: int | None = None
    if config.storage.max_dataset_size_gb is not None:
        max_ds_bytes = int(config.storage.max_dataset_size_gb * 1024**3)
    policy = _build_policy(
        config,
        registries,
        fresh_download=fresh_download,
        retry_failed=retry_failed,
        max_dataset_size_bytes=max_ds_bytes,
    )

    progress_bus_factory = registries.progress_buses.get("default")
    progress_bus = progress_bus_factory()

    manifest_factory = registries.manifests.get("default")
    manifests = manifest_factory(runs_dir)

    path_strategy_factory = registries.path_strategies.get("default")
    path_strategy = path_strategy_factory(config.storage.layout)

    filesystem_factory = registries.filesystems.get("default")
    filesystem = filesystem_factory(Path(config.storage.downloads_root))

    checkpoint = CheckpointManager(
        _path=runs_dir,
        _pipeline_id=config.pipeline.id,
    )

    return Container(
        config=config,
        run_id=run_id,
        logger_bundle=logger_bundle,
        auth=auth,
        http_client=http_client,
        rate_limiter=rate_limiter,
        extractor=extractor,
        pre_transform_chain=pre_transform_chain,
        post_transform_chain=post_transform_chain,
        downloader=downloader,
        sink=sink,
        policy=policy,
        progress_bus=progress_bus,
        manifests=manifests,
        path_strategy=path_strategy,
        filesystem=filesystem,
        config_hash=config_hash(config),
        checkpoint=checkpoint,
    )


def _build_auth(config: PipelineConfig, registries: ComponentRegistries) -> AuthProvider:
    factory = registries.auth.get(config.auth.type)
    return factory(config)  # type: ignore[no-any-return]


def _build_extractor(
    config: PipelineConfig,
    http_client: HttpxClient,
    auth: AuthProvider,
    registries: ComponentRegistries,
) -> Extractor:
    factory = registries.extractors.get(config.extractor.name)
    return factory(http_client, auth, config.extractor.options)  # type: ignore[no-any-return]


def _build_transforms(
    settings: list[TransformSettings], registries: ComponentRegistries
) -> TransformChain:
    transforms: list[Transform] = []
    for t in settings:
        factory = registries.transforms.get(t.name)
        transforms.append(factory(t.name, t.options))
    return TransformChain(transforms=transforms)


def _build_sink(config: PipelineConfig, registries: ComponentRegistries) -> Sink:
    factory = registries.sinks.get(config.sink.type)
    return factory(config.sink.options)  # type: ignore[no-any-return]


def _build_policy(
    config: PipelineConfig,
    registries: ComponentRegistries,
    *,
    fresh_download: bool,
    retry_failed: bool,
    max_dataset_size_bytes: int | None = None,
) -> Policy:
    if retry_failed:
        factory = registries.policies.get("retry")
        return factory(retry_failed, max_dataset_size_bytes)  # type: ignore[no-any-return]
    factory = registries.policies.get("incremental")
    return factory(config.pipeline.run_mode, fresh_download, max_dataset_size_bytes)  # type: ignore[no-any-return]
