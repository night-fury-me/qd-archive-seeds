from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord

if TYPE_CHECKING:
    from qdarchive_seeding.app.config_models import PipelineConfig


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------


class HttpResponse(Protocol):
    """Protocol for HTTP response objects (satisfied by httpx.Response)."""

    status_code: int
    text: str

    def json(self) -> Any: ...

    def raise_for_status(self) -> None: ...


class HttpClient(Protocol):
    async def get(
        self,
        url: str,
        *,
        headers: dict[str, str],
        params: dict[str, Any],
        timeout: float | None = None,
    ) -> HttpResponse: ...

    async def get_many(
        self,
        requests: list[dict[str, Any]],
    ) -> list[HttpResponse]: ...


class AuthProvider(Protocol):
    def apply(
        self, headers: dict[str, str], params: dict[str, Any]
    ) -> tuple[dict[str, str], dict[str, Any]]: ...


# ---------------------------------------------------------------------------
# Checkpoint / Resume
# ---------------------------------------------------------------------------


class Checkpoint(Protocol):
    """Protocol for checkpoint/resume state management used by extractors."""

    def is_query_complete(self, query_label: str) -> bool: ...

    def get_failed_pages(self, query_label: str) -> list[int]: ...

    def clear_failed_page(self, query_label: str, page: int) -> None: ...

    def mark_query_complete(self, query_label: str) -> None: ...

    def get_start_page(self, query_label: str) -> int: ...

    def mark_page_failed(self, query_label: str, page: int) -> None: ...

    def mark_page(self, query_label: str, page: int, records: int) -> None: ...

    def get_date_slices(self, query_label: str) -> list[tuple[str, str]] | None: ...

    def set_date_slices(self, query_label: str, slices: list[tuple[str, str]]) -> None: ...


# ---------------------------------------------------------------------------
# Progress
# ---------------------------------------------------------------------------


class ProgressBus(Protocol):
    def publish(self, event: Any) -> None: ...


# ---------------------------------------------------------------------------
# Pipeline components
# ---------------------------------------------------------------------------


class RunContext(Protocol):
    run_id: str
    pipeline_id: str
    config: PipelineConfig
    cancelled: bool
    progress_bus: ProgressBus | None
    checkpoint: Checkpoint | None
    existing_dataset_ids: set[str] | None


class Extractor(Protocol):
    def extract(self, ctx: RunContext) -> AsyncIterator[DatasetRecord]: ...


class Transform(Protocol):
    name: str

    def apply(self, record: DatasetRecord) -> DatasetRecord | None: ...


class Sink(Protocol):
    def upsert_dataset(self, record: DatasetRecord) -> str: ...

    def upsert_asset(self, dataset_id: str, asset: AssetRecord) -> None: ...

    def close(self) -> None: ...


@runtime_checkable
class ResumableSink(Protocol):
    """Sink that supports resume by querying previously stored state."""

    def get_existing_dataset_ids(self, repository_id: int) -> set[str]: ...

    def get_pending_download_datasets(
        self, repository_id: int | None = None
    ) -> list[tuple[str, DatasetRecord, list[AssetRecord]]]: ...

    def get_file_statuses(self, dataset_id: str) -> dict[str, str]: ...


class DownloadResult(Protocol):
    """Protocol for download results."""

    asset: AssetRecord
    bytes_downloaded: int
    checksum: str | None


class Downloader(Protocol):
    async def download(
        self,
        asset: AssetRecord,
        target_dir: Path,
        *,
        progress_callback: object = None,
    ) -> DownloadResult: ...

    async def close(self) -> None: ...


class Policy(Protocol):
    def should_skip_dataset(self, record: DatasetRecord) -> bool: ...

    def should_skip_asset(self, asset: AssetRecord) -> bool: ...

    def should_retry(self, error: Exception, attempt: int) -> bool: ...


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

class Registry(Protocol):
    def register(self, name: str, component: Any, spec: ComponentSpec | None = None) -> None: ...

    def get(self, name: str) -> Any: ...

    def spec(self, name: str) -> ComponentSpec | None: ...

    def list(self) -> list[str]: ...


# ---------------------------------------------------------------------------
# Config / Validation / Specs
# ---------------------------------------------------------------------------


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


class ConfigLoader(Protocol):
    def load(self, path: str | Path) -> PipelineConfig: ...


class ConfigValidator(Protocol):
    def validate(self, config: PipelineConfig) -> ValidationResult: ...
