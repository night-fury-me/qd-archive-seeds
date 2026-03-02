from __future__ import annotations

from pathlib import Path

import pytest

from qdarchive_seeding.app.config_models import PipelineConfig
from qdarchive_seeding.app.registry import InMemoryRegistry, create_default_registries
from qdarchive_seeding.core.exceptions import RegistryError
import httpx

from qdarchive_seeding.infra.http.auth import ApiKeyAuth, BearerAuth, OAuth2ClientCredentials
from qdarchive_seeding.infra.storage.checksums import ChecksumComputer
from qdarchive_seeding.infra.storage.downloader import Downloader
from qdarchive_seeding.app.manifests import RunManifestWriter
from qdarchive_seeding.app.progress import ProgressBus
from qdarchive_seeding.infra.storage.filesystem import FileSystem
from qdarchive_seeding.infra.storage.paths import PathStrategy
from qdarchive_seeding.infra.extractors.generic_rest import GenericRestExtractor
from qdarchive_seeding.infra.extractors.zenodo import ZenodoExtractor
from qdarchive_seeding.infra.extractors.html_scraper import HtmlScraperExtractor
from qdarchive_seeding.infra.extractors.syracuse_qdr import SyracuseQdrExtractor
from qdarchive_seeding.infra.extractors.static_list import StaticListExtractor
from qdarchive_seeding.infra.sinks.csv_sink import CSVSink
from qdarchive_seeding.infra.sinks.excel_sink import ExcelSink
from qdarchive_seeding.infra.sinks.sqlite import SQLiteSink
from qdarchive_seeding.infra.transforms.filter_by_extensions import FilterByExtensions
from qdarchive_seeding.infra.transforms.validate_required import ValidateRequiredFields
from qdarchive_seeding.infra.transforms.normalize_fields import NormalizeFields
from qdarchive_seeding.infra.transforms.infer_filetypes import InferFileTypes
from qdarchive_seeding.infra.transforms.deduplicate_assets import DeduplicateAssets
from qdarchive_seeding.infra.transforms.slugify_dataset import SlugifyDataset
from qdarchive_seeding.infra.transforms.classify_qda_files import ClassifyQdaFiles


def test_register_and_get() -> None:
    reg = InMemoryRegistry()
    reg.register("foo", "bar_component")
    assert reg.get("foo") == "bar_component"


def test_register_duplicate_raises() -> None:
    reg = InMemoryRegistry()
    reg.register("foo", "bar")
    with pytest.raises(RegistryError, match="already registered"):
        reg.register("foo", "baz")


def test_get_missing_raises() -> None:
    reg = InMemoryRegistry()
    with pytest.raises(RegistryError, match="not found"):
        reg.get("missing")


def test_list_returns_sorted() -> None:
    reg = InMemoryRegistry()
    reg.register("zebra", 1)
    reg.register("alpha", 2)
    assert reg.list() == ["alpha", "zebra"]


def test_spec_returns_none_by_default() -> None:
    reg = InMemoryRegistry()
    reg.register("foo", "bar")
    assert reg.spec("foo") is None


def test_spec_missing_raises() -> None:
    reg = InMemoryRegistry()
    with pytest.raises(RegistryError, match="not found"):
        reg.spec("missing")


def test_create_default_registries_smoke() -> None:
    registries = create_default_registries()
    assert "sqlite" in registries.sinks.list()
    assert "csv" in registries.sinks.list()
    assert "excel" in registries.sinks.list()
    assert "generic_rest_extractor" in registries.extractors.list()


def test_registry_auth_factories(minimal_config: PipelineConfig, monkeypatch: object) -> None:
    registries = create_default_registries()

    monkeypatch.setenv("API_KEY_ENV", "secret")  # type: ignore[attr-defined]
    minimal_config.auth.type = "api_key"
    minimal_config.auth.env = {"api_key": "API_KEY_ENV"}
    auth = registries.auth.get("api_key")(minimal_config)
    assert isinstance(auth, ApiKeyAuth)
    assert auth.api_key == "secret"

    minimal_config.auth.type = "bearer"
    minimal_config.auth.env = {"token": "TOKEN_ENV"}
    monkeypatch.setenv("TOKEN_ENV", "tok")  # type: ignore[attr-defined]
    auth = registries.auth.get("bearer")(minimal_config)
    assert isinstance(auth, BearerAuth)
    assert auth.token == "tok"

    minimal_config.auth.type = "oauth2"
    minimal_config.auth.env = {
        "token_url": "TOKEN_URL",
        "client_id": "CLIENT_ID",
        "client_secret": "CLIENT_SECRET",
        "scope": "SCOPE",
    }
    monkeypatch.setenv("TOKEN_URL", "https://example.com/token")  # type: ignore[attr-defined]
    monkeypatch.setenv("CLIENT_ID", "client")  # type: ignore[attr-defined]
    monkeypatch.setenv("CLIENT_SECRET", "secret")  # type: ignore[attr-defined]
    monkeypatch.setenv("SCOPE", "read")  # type: ignore[attr-defined]
    auth = registries.auth.get("oauth2")(minimal_config)
    assert isinstance(auth, OAuth2ClientCredentials)
    assert auth.token_url == "https://example.com/token"
    assert auth.client_id == "client"


def test_registry_extractor_factories() -> None:
    registries = create_default_registries()

    class DummyClient:
        pass

    class DummyAuth:
        def apply(self, headers, params):
            return headers, params

    client = DummyClient()
    auth = DummyAuth()

    zenodo = registries.extractors.get("zenodo_extractor")(client, auth, {})
    assert isinstance(zenodo, ZenodoExtractor)

    generic = registries.extractors.get("generic_rest_extractor")(client, auth, {})
    assert isinstance(generic, GenericRestExtractor)

    html = registries.extractors.get("html_scraper_extractor")(client, auth, {})
    assert isinstance(html, HtmlScraperExtractor)

    syracuse = registries.extractors.get("syracuse_qdr_extractor")(client, auth, {})
    assert isinstance(syracuse, SyracuseQdrExtractor)

    static = registries.extractors.get("static_list_extractor")(client, auth, {})
    assert isinstance(static, StaticListExtractor)


def test_registry_transform_factory_defaults() -> None:
    registries = create_default_registries()
    transform = registries.transforms.get("filter_by_extensions")(
        "filter_by_extensions", {"categories": "bad", "extra_extensions": "bad"}
    )
    assert isinstance(transform, FilterByExtensions)
    assert transform.categories == ["analysis_data"]
    assert transform.extra_extensions == []


def test_registry_transform_factories() -> None:
    registries = create_default_registries()

    validate = registries.transforms.get("validate_required_fields")(
        "validate_required_fields", {"required_fields": "bad"}
    )
    assert isinstance(validate, ValidateRequiredFields)
    assert validate.required_fields == []

    normalize = registries.transforms.get("normalize_fields")("normalize_fields", {})
    assert isinstance(normalize, NormalizeFields)

    infer = registries.transforms.get("infer_filetypes")("infer_filetypes", {})
    assert isinstance(infer, InferFileTypes)

    dedupe = registries.transforms.get("deduplicate_assets")("deduplicate_assets", {})
    assert isinstance(dedupe, DeduplicateAssets)

    slugify = registries.transforms.get("slugify_dataset")("slugify_dataset", {})
    assert isinstance(slugify, SlugifyDataset)

    classify = registries.transforms.get("classify_qda_files")("classify_qda_files", {})
    assert isinstance(classify, ClassifyQdaFiles)


def test_registry_sink_factories(tmp_path: Path, monkeypatch: object) -> None:
    registries = create_default_registries()

    sqlite_sink = registries.sinks.get("sqlite")({"path": str(tmp_path / "db.sqlite")})
    assert isinstance(sqlite_sink, SQLiteSink)
    sqlite_sink.close()

    csv_sink = registries.sinks.get("csv")(
        {"dataset_path": str(tmp_path / "ds.csv"), "asset_path": str(tmp_path / "as.csv")}
    )
    assert isinstance(csv_sink, CSVSink)

    excel_sink = registries.sinks.get("excel")({"path": str(tmp_path / "db.xlsx")})
    assert isinstance(excel_sink, ExcelSink)

    import qdarchive_seeding.infra.sinks.mysql as mysql_module
    import qdarchive_seeding.infra.sinks.mongodb as mongo_module

    class DummyMySQLSink(mysql_module.MySQLSink):
        def __post_init__(self) -> None:
            return None

    class DummyMongoDBSink(mongo_module.MongoDBSink):
        def __post_init__(self) -> None:
            return None

    monkeypatch.setattr(mysql_module, "MySQLSink", DummyMySQLSink)
    monkeypatch.setattr(mongo_module, "MongoDBSink", DummyMongoDBSink)

    mysql_sink = registries.sinks.get("mysql")({"password_env": "DB_PASS"})
    assert isinstance(mysql_sink, DummyMySQLSink)

    mongo_sink = registries.sinks.get("mongodb")({"uri": "mongodb://example"})
    assert isinstance(mongo_sink, DummyMongoDBSink)


def test_registry_misc_factories(tmp_path: Path) -> None:
    registries = create_default_registries()

    checksum = registries.checksums.get("default")("sha256")
    assert isinstance(checksum, ChecksumComputer)

    downloader = registries.downloaders.get("default")(httpx.Client(), checksum, 8)
    assert isinstance(downloader, Downloader)
    downloader.client.close()

    progress_bus = registries.progress_buses.get("default")()
    assert isinstance(progress_bus, ProgressBus)

    manifests = registries.manifests.get("default")(tmp_path)
    assert isinstance(manifests, RunManifestWriter)

    path_strategy = registries.path_strategies.get("default")("{source_name}/{dataset_slug}/")
    assert isinstance(path_strategy, PathStrategy)

    filesystem = registries.filesystems.get("default")(tmp_path)
    assert isinstance(filesystem, FileSystem)
