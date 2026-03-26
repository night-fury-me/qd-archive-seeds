from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from qdarchive_seeding.app.checkpoint import CheckpointManager
from qdarchive_seeding.app.config_models import PipelineConfig
from qdarchive_seeding.infra.extractors.zenodo import ZenodoExtractor, ZenodoOptions
from qdarchive_seeding.infra.http.auth import NoAuth

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FakeResponse:
    _json: Any

    def json(self) -> Any:
        return self._json

    def raise_for_status(self) -> None:
        pass


class FakeHttpClient:
    """HttpClient that can fail on specific calls."""

    def __init__(
        self,
        responses: list[Any],
        *,
        fail_on_call: set[int] | None = None,
    ) -> None:
        self._responses = list(responses)
        self._empty: dict[str, Any] = {"hits": {"hits": [], "total": 0}}
        self._fail_on_call = fail_on_call or set()
        self.call_count = 0
        self.calls: list[dict[str, Any]] = []

    async def get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> FakeResponse:
        idx = self.call_count
        self.call_count += 1
        self.calls.append({"url": url, "headers": headers, "params": params})
        if idx in self._fail_on_call:
            raise ConnectionError(f"Simulated failure on call {idx}")
        payload = self._responses.pop(0) if self._responses else self._empty
        return FakeResponse(_json=payload)


@dataclass(slots=True)
class FakeRunContext:
    run_id: str = "test-run"
    pipeline_id: str = "test_pipeline"
    config: PipelineConfig = field(default=None)  # type: ignore[assignment]
    cancelled: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


def _make_config() -> PipelineConfig:
    return PipelineConfig.model_validate(
        {
            "pipeline": {"id": "test_pipeline", "run_mode": "incremental"},
            "source": {
                "name": "zenodo",
                "type": "rest_api",
                "base_url": "https://zenodo.org/api",
                "endpoints": {"search": "/records"},
                "params": {"size": 2, "q": ""},
            },
            "auth": {"type": "none"},
            "extractor": {"name": "zenodo_extractor", "options": {}},
            "transforms": [],
            "storage": {"downloads_root": "./downloads", "layout": "{source_name}/{dataset_slug}/"},
            "sink": {"type": "sqlite", "options": {"path": "./test.sqlite"}},
            "logging": {"level": "WARNING"},
        }
    )


def _hit(record_id: int) -> dict[str, Any]:
    """Build a minimal Zenodo API hit."""
    return {
        "id": record_id,
        "metadata": {
            "title": f"Record {record_id}",
            "creators": [{"name": "Author"}],
        },
        "links": {"self": f"https://zenodo.org/api/records/{record_id}"},
        "files": [],
    }


def _page(*record_ids: int, total: int = 100) -> dict[str, Any]:
    return {"hits": {"hits": [_hit(rid) for rid in record_ids], "total": total}}


EMPTY_PAGE: dict[str, Any] = {"hits": {"hits": [], "total": 0}}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFailedPageRecording:
    """When a page fetch fails, it should be recorded in the checkpoint."""

    @pytest.mark.asyncio
    async def test_failed_page_recorded_in_checkpoint(self, tmp_path: Path) -> None:
        # Page 0 succeeds, page 1 fails, page 2 succeeds, page 3 empty (stops)
        http = FakeHttpClient(
            [_page(1, 2, total=6), _page(5, 6, total=6), EMPTY_PAGE],
            fail_on_call={1},  # Fail on call index 1 (page 1)
        )
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test_pipeline")
        config = _make_config()
        ctx = FakeRunContext(config=config, metadata={"checkpoint": cp})

        extractor = ZenodoExtractor(
            http_client=http, auth=NoAuth(), options=ZenodoOptions(include_files=False)
        )
        records = [
            r async for r in extractor._extract_single_query(ctx, "test query", query_string="q1")
        ]

        # Page 0 yielded 2, page 1 failed (skipped), page 2 yielded 2
        assert len(records) == 4
        assert cp.get_failed_pages("q1") == [1]

    @pytest.mark.asyncio
    async def test_query_not_marked_complete_with_failures(self, tmp_path: Path) -> None:
        http = FakeHttpClient(
            [_page(1, total=2), EMPTY_PAGE],
            fail_on_call={1},  # Fail on the empty-page call (page 1)
        )
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test_pipeline")
        config = _make_config()
        ctx = FakeRunContext(config=config, metadata={"checkpoint": cp})

        extractor = ZenodoExtractor(
            http_client=http, auth=NoAuth(), options=ZenodoOptions(include_files=False)
        )
        [r async for r in extractor._extract_single_query(ctx, "test query", query_string="q1")]

        assert not cp.is_query_complete("q1")
        assert cp.get_failed_pages("q1") == [1]


class TestRetryFailedPages:
    """On resume, failed pages should be retried before normal pagination."""

    @pytest.mark.asyncio
    async def test_retry_succeeds_yields_records(self, tmp_path: Path) -> None:
        # Set up checkpoint with a failed page
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test_pipeline")
        cp.mark_page("q1", 2, 4)  # Pages 0-1 were done, page 2 is resume_from
        cp.mark_page_failed("q1", 1)  # Page 1 failed previously

        # HTTP responses: retry of page 1 succeeds, then empty page for normal pagination
        http = FakeHttpClient([_page(10, 11, total=6), EMPTY_PAGE])
        config = _make_config()
        ctx = FakeRunContext(config=config, metadata={"checkpoint": cp})

        extractor = ZenodoExtractor(
            http_client=http, auth=NoAuth(), options=ZenodoOptions(include_files=False)
        )
        records = [
            r async for r in extractor._extract_single_query(ctx, "test query", query_string="q1")
        ]

        # Should have yielded the 2 records from the retried page
        assert len(records) == 2
        assert records[0].source_dataset_id == "10"
        assert records[1].source_dataset_id == "11"
        # Failed page should be cleared
        assert cp.get_failed_pages("q1") == []

    @pytest.mark.asyncio
    async def test_retry_still_failing_stays_in_checkpoint(self, tmp_path: Path) -> None:
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test_pipeline")
        cp.mark_page("q1", 2, 4)
        cp.mark_page_failed("q1", 1)

        # Retry also fails (call 0), then empty page for normal pagination
        http = FakeHttpClient([EMPTY_PAGE], fail_on_call={0})
        config = _make_config()
        ctx = FakeRunContext(config=config, metadata={"checkpoint": cp})

        extractor = ZenodoExtractor(
            http_client=http, auth=NoAuth(), options=ZenodoOptions(include_files=False)
        )
        records = [
            r async for r in extractor._extract_single_query(ctx, "test query", query_string="q1")
        ]

        assert len(records) == 0
        # Page should remain in failed list
        assert cp.get_failed_pages("q1") == [1]

    @pytest.mark.asyncio
    async def test_retry_happens_before_normal_pagination(self, tmp_path: Path) -> None:
        """Verify ordering: retry calls happen before resume pagination calls."""
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test_pipeline")
        cp.mark_page("q1", 1, 2)  # Normal pagination resumes from page 1
        cp.mark_page_failed("q1", 0)  # Page 0 failed

        # Call 0: retry of page 0, Call 1: normal pagination page 1, Call 2: empty
        http = FakeHttpClient(
            [
                _page(1, 2, total=4),  # retry result
                _page(3, 4, total=4),  # normal pagination result
                EMPTY_PAGE,
            ]
        )
        config = _make_config()
        ctx = FakeRunContext(config=config, metadata={"checkpoint": cp})

        extractor = ZenodoExtractor(
            http_client=http, auth=NoAuth(), options=ZenodoOptions(include_files=False)
        )
        records = [
            r async for r in extractor._extract_single_query(ctx, "test query", query_string="q1")
        ]

        assert len(records) == 4
        # First 2 from retry, next 2 from normal pagination
        assert records[0].source_dataset_id == "1"
        assert records[2].source_dataset_id == "3"
        # Retry call should have page=1 (0-indexed page 0 → 1-indexed)
        assert http.calls[0]["params"]["page"] == 1


class TestDateSliceCache:
    """Date slices should be cached in checkpoint and reused on resume."""

    @pytest.mark.asyncio
    async def test_date_slices_cached_after_computation(self, tmp_path: Path) -> None:
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test_pipeline")
        assert cp.get_date_slices("ext batch 1") is None

        slices = [("2013-01-01", "2020-06-15"), ("2020-06-16", "2026-03-24")]
        cp.set_date_slices("ext batch 1", slices)

        # Reload and verify
        cp2 = CheckpointManager(_path=tmp_path, _pipeline_id="test_pipeline")
        assert cp2.get_date_slices("ext batch 1") == slices
