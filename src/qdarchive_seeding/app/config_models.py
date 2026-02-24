from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class BaseConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PipelineSettings(BaseConfig):
    id: str
    run_mode: Literal["incremental", "full"] = "incremental"
    max_items: int | None = None


class PaginationSettings(BaseConfig):
    type: Literal["page", "cursor", "offset"]
    page_param: str | None = None
    size_param: str | None = None
    cursor_param: str | None = None
    offset_param: str | None = None


class SourceSettings(BaseConfig):
    name: str
    type: Literal["rest_api", "html", "static_list"]
    base_url: str
    endpoints: dict[str, str] = Field(default_factory=dict)
    params: dict[str, Any] = Field(default_factory=dict)
    pagination: PaginationSettings | None = None


class AuthSettings(BaseConfig):
    type: Literal["none", "api_key", "bearer", "oauth2"]
    env: dict[str, str] = Field(default_factory=dict)
    placement: Literal["header", "query"] | None = None
    header_name: str | None = None


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
    transforms: list[TransformSettings] = Field(default_factory=list)
    storage: StorageSettings
    sink: SinkSettings
    logging: LoggingSettings
