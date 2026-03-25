from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord

if TYPE_CHECKING:
    from qdarchive_seeding.app.config_models import PipelineConfig


class Extractor(Protocol):
    def extract(self, ctx: RunContext) -> AsyncIterator[DatasetRecord]: ...


class Transform(Protocol):
    name: str

    def apply(self, record: DatasetRecord) -> DatasetRecord | None: ...


class Sink(Protocol):
    def upsert_dataset(self, record: DatasetRecord) -> str: ...

    def upsert_asset(self, dataset_id: str, asset: AssetRecord) -> None: ...

    def close(self) -> None: ...


class Downloader(Protocol):
    async def download(self, asset: AssetRecord, target_dir: Any) -> Any: ...


class AuthProvider(Protocol):
    def apply(
        self, headers: dict[str, str], params: dict[str, Any]
    ) -> tuple[dict[str, str], dict[str, Any]]: ...


class HttpClient(Protocol):
    async def get(
        self,
        url: str,
        *,
        headers: dict[str, str],
        params: dict[str, Any],
        timeout: float | None = None,
    ) -> Any: ...

    async def get_many(
        self,
        requests: list[dict[str, Any]],
    ) -> list[Any]: ...


class RunContext(Protocol):
    run_id: str
    pipeline_id: str
    config: PipelineConfig
    cancelled: bool
    metadata: dict[str, Any]


class ProgressBus(Protocol):
    def publish(self, event: Any) -> None: ...


class ValidationResult(Protocol):
    ok: bool
    errors: list[str]
    warnings: list[str]


class ComponentSpec(Protocol):
    name: str
    description: str
    fields: list[FieldSpec]

    def validate(self, config: dict[str, Any]) -> list[str]: ...


class FieldSpec(Protocol):
    key: str
    type: str
    default: Any
    required: bool
    help: str | None
    choices: list[str] | None
    secret: bool


class Registry(Protocol):
    def register(self, name: str, component: Any, spec: ComponentSpec | None = None) -> None: ...

    def get(self, name: str) -> Any: ...

    def spec(self, name: str) -> ComponentSpec | None: ...

    def list(self) -> list[str]: ...


class ConfigLoader(Protocol):
    def load(self, path: str | Path) -> PipelineConfig: ...


class ConfigValidator(Protocol):
    def validate(self, config: PipelineConfig) -> ValidationResult: ...


class Policy(ABC):
    @abstractmethod
    def should_skip_asset(self, asset: AssetRecord) -> bool: ...

    @abstractmethod
    def should_retry(self, error: Exception, attempt: int) -> bool: ...
