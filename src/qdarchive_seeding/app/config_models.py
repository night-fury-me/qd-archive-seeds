from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class BaseConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PipelineSettings(BaseConfig):
    id: str
    run_mode: Literal["incremental", "full"] = "incremental"
    max_items: int | None = None
    phases: list[Literal["metadata", "download"]] = ["metadata", "download"]


class PaginationSettings(BaseConfig):
    type: Literal["page", "cursor", "offset"]
    page_param: str | None = None
    size_param: str | None = None
    cursor_param: str | None = None
    offset_param: str | None = None


class HttpSettings(BaseConfig):
    timeout_seconds: float = 30.0
    max_retries: int = 3
    backoff_min: float = 0.5
    backoff_max: float = 6.0
    rate_limit_per_second: float = 5.0


class SearchStrategy(BaseConfig):
    """Multi-query search configuration.

    When set on a source, the extractor will iterate over all extension
    and natural-language queries, deduplicating results across them.

    ``base_query_prefix`` is prepended to the ``q`` param for extension queries.
    ``facet_filters`` are added as separate API params (e.g. ``type: dataset``)
    and apply to both extension and NL queries.
    """

    extension_queries: list[str] = Field(default_factory=list)
    natural_language_queries: list[str] = Field(default_factory=list)
    base_query_prefix: str = ""
    facet_filters: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _coerce_nulls(cls, values: dict[str, object]) -> dict[str, object]:
        """YAML returns None for keys with no value; coerce to empty list/dict."""
        for key in ("extension_queries", "natural_language_queries"):
            if values.get(key) is None:
                values[key] = []
        if values.get("facet_filters") is None:
            values["facet_filters"] = {}
        return values


class SourceSettings(BaseConfig):
    name: str
    type: Literal["rest_api", "html", "static_list"]
    base_url: str = ""
    endpoints: dict[str, str] = Field(default_factory=dict)
    params: dict[str, Any] = Field(default_factory=dict)
    pagination: PaginationSettings | None = None
    search_strategy: SearchStrategy | None = None
    repository_id: int | None = None
    repository_url: str | None = None


class AuthSettings(BaseConfig):
    type: Literal["none", "api_key", "bearer", "oauth2"]
    env: dict[str, str] = Field(default_factory=dict)
    placement: Literal["header", "query"] | None = None
    header_name: str | None = None


class ExternalAuthEntry(BaseConfig):
    """Auth config for an external Dataverse host (used for harvested datasets)."""

    type: Literal["api_key", "bearer"] = "api_key"
    env: dict[str, str] = Field(default_factory=dict)
    header_name: str = "X-Dataverse-key"


class ExtractorSettings(BaseConfig):
    name: str
    options: dict[str, Any] = Field(default_factory=dict)


class TransformSettings(BaseConfig):
    name: str
    options: dict[str, Any] = Field(default_factory=dict)


class StorageSettings(BaseConfig):
    downloads_root: str
    layout: str
    checksum: Literal["sha256", "none"] = "sha256"
    chunk_size_bytes: int | None = None


class SinkSettings(BaseConfig):
    type: Literal["sqlite", "mysql", "mongodb", "csv", "excel"]
    options: dict[str, Any] = Field(default_factory=dict)


class ConsoleLoggingSettings(BaseConfig):
    enabled: bool = True
    rich: bool = True


class FileLoggingSettings(BaseConfig):
    enabled: bool = False
    path: str | None = None


class LoggingSettings(BaseConfig):
    level: str = "INFO"
    console: ConsoleLoggingSettings = Field(default_factory=ConsoleLoggingSettings)
    file: FileLoggingSettings = Field(default_factory=FileLoggingSettings)


class PipelineConfig(BaseConfig):
    pipeline: PipelineSettings
    source: SourceSettings
    auth: AuthSettings
    extractor: ExtractorSettings
    pre_transforms: list[TransformSettings] = Field(default_factory=list)
    post_transforms: list[TransformSettings] = Field(default_factory=list)
    storage: StorageSettings
    sink: SinkSettings
    http: HttpSettings = Field(default_factory=HttpSettings)
    logging: LoggingSettings
    external_auth: dict[str, ExternalAuthEntry] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _migrate_transforms(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Auto-migrate old ``transforms:`` key to ``pre_transforms:``."""
        if isinstance(data, dict) and "transforms" in data:
            if "pre_transforms" not in data:
                data["pre_transforms"] = data.pop("transforms")
            else:
                data.pop("transforms")
        return data
