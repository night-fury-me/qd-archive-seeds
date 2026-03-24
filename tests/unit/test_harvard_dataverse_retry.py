from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from qdarchive_seeding.app.checkpoint import CheckpointManager
from qdarchive_seeding.app.config_models import PipelineConfig
from qdarchive_seeding.infra.extractors.harvard_dataverse import (
    HarvardDataverseExtractor,
    HarvardDataverseOptions,
)
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
    def __init__(
        self,
        responses: list[Any],
        *,
        fail_on_call: set[int] | None = None,
    ) -> None:
        self._responses = list(responses)
        self._empty: dict[str, Any] = {"data": {"items": [], "total_count": 0}}
        self._fail_on_call = fail_on_call or set()
        self.call_count = 0
        self.calls: list[dict[str, Any]] = []

    def get(
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
                "name": "harvard-dataverse",
                "type": "rest_api",
                "base_url": "https://dataverse.harvard.edu/api",
                "endpoints": {
                    "search": "/search",
                    "files": "/datasets/:persistentId/versions/:latest/files",
                },
                "params": {},
            },
            "auth": {"type": "none"},
            "extractor": {"name": "harvard_dataverse_extractor", "options": {}},
            "transforms": [],
            "storage": {"downloads_root": "./downloads", "layout": "{source_name}/{dataset_slug}/"},
            "sink": {"type": "sqlite", "options": {"path": "./test.sqlite"}},
            "logging": {"level": "WARNING"},
        }
    )


def _item(global_id: str, total_count: int = 100) -> dict[str, Any]:
    return {
        "global_id": global_id,
        "name": f"Dataset {global_id}",
        "url": f"https://dataverse.harvard.edu/dataset.xhtml?persistentId={global_id}",
        "authors": ["Author One"],
    }


def _page(*global_ids: str, total_count: int = 100) -> dict[str, Any]:
    return {
        "data": {
            "items": [_item(gid) for gid in global_ids],
            "total_count": total_count,
        }
    }


EMPTY_PAGE: dict[str, Any] = {"data": {"items": [], "total_count": 0}}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFailedPageRecording:
    def test_failed_page_recorded_in_checkpoint(self, tmp_path: Path) -> None:
        # per_page=2, total_count=6: pages 0,1,2. Page 1 fails.
        http = FakeHttpClient(
            [
                _page("doi:1", "doi:2", total_count=6),  # page 0
                _page("doi:5", "doi:6", total_count=6),  # page 2 (after page 1 fails)
            ],
            fail_on_call={1},  # page 1 fails
        )
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test_pipeline")
        config = _make_config()
        ctx = FakeRunContext(config=config, metadata={"checkpoint": cp})

        extractor = HarvardDataverseExtractor(
            http_client=http,
            auth=NoAuth(),
            options=HarvardDataverseOptions(include_files=False, per_page=2),
        )
        records = list(extractor._extract_single_query(ctx, "test", query_string="q1"))

        assert len(records) == 4  # Page 0 + page 2
        assert cp.get_failed_pages("q1") == [1]

    def test_query_not_marked_complete_with_failures(self, tmp_path: Path) -> None:
        # per_page=2, total_count=4: pages 0,1. Page 1 fails.
        http = FakeHttpClient(
            [_page("doi:1", "doi:2", total_count=4)],
            fail_on_call={1},  # page 1 fails
        )
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test_pipeline")
        config = _make_config()
        ctx = FakeRunContext(config=config, metadata={"checkpoint": cp})

        extractor = HarvardDataverseExtractor(
            http_client=http,
            auth=NoAuth(),
            options=HarvardDataverseOptions(include_files=False, per_page=2),
        )
        list(extractor._extract_single_query(ctx, "test", query_string="q1"))

        assert not cp.is_query_complete("q1")
        assert cp.get_failed_pages("q1") == [1]


class TestRetryFailedPages:
    def test_retry_succeeds_yields_records(self, tmp_path: Path) -> None:
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test_pipeline")
        cp.mark_page("q1", 2, 4)
        cp.mark_page_failed("q1", 1)

        http = FakeHttpClient([_page("doi:10", "doi:11", total_count=6), EMPTY_PAGE])
        config = _make_config()
        ctx = FakeRunContext(config=config, metadata={"checkpoint": cp})

        extractor = HarvardDataverseExtractor(
            http_client=http, auth=NoAuth(), options=HarvardDataverseOptions(include_files=False)
        )
        records = list(extractor._extract_single_query(ctx, "test", query_string="q1"))

        assert len(records) == 2
        assert records[0].source_dataset_id == "doi:10"
        assert cp.get_failed_pages("q1") == []

    def test_retry_still_failing_stays_in_checkpoint(self, tmp_path: Path) -> None:
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test_pipeline")
        cp.mark_page("q1", 2, 4)
        cp.mark_page_failed("q1", 1)

        http = FakeHttpClient([EMPTY_PAGE], fail_on_call={0})
        config = _make_config()
        ctx = FakeRunContext(config=config, metadata={"checkpoint": cp})

        extractor = HarvardDataverseExtractor(
            http_client=http, auth=NoAuth(), options=HarvardDataverseOptions(include_files=False)
        )
        records = list(extractor._extract_single_query(ctx, "test", query_string="q1"))

        assert len(records) == 0
        assert cp.get_failed_pages("q1") == [1]

    def test_retry_happens_before_normal_pagination(self, tmp_path: Path) -> None:
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test_pipeline")
        cp.mark_page("q1", 1, 2)
        cp.mark_page_failed("q1", 0)

        http = FakeHttpClient([
            _page("doi:1", "doi:2", total_count=4),  # retry
            _page("doi:3", "doi:4", total_count=4),  # normal pagination
            EMPTY_PAGE,
        ])
        config = _make_config()
        ctx = FakeRunContext(config=config, metadata={"checkpoint": cp})

        extractor = HarvardDataverseExtractor(
            http_client=http, auth=NoAuth(), options=HarvardDataverseOptions(include_files=False)
        )
        records = list(extractor._extract_single_query(ctx, "test", query_string="q1"))

        assert len(records) == 4
        assert records[0].source_dataset_id == "doi:1"
        assert records[2].source_dataset_id == "doi:3"
        # Retry should use start=0 (page 0 * per_page)
        assert http.calls[0]["params"]["start"] == 0


class TestResumeSkipsCompletedQueries:
    def test_completed_query_is_skipped(self, tmp_path: Path) -> None:
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test_pipeline")
        cp.mark_query_complete("q1")

        http = FakeHttpClient([_page("doi:1", total_count=1)])
        config = _make_config()
        ctx = FakeRunContext(config=config, metadata={"checkpoint": cp})

        extractor = HarvardDataverseExtractor(
            http_client=http, auth=NoAuth(), options=HarvardDataverseOptions(include_files=False)
        )
        records = list(extractor._extract_single_query(ctx, "test", query_string="q1"))

        assert len(records) == 0
        assert http.call_count == 0  # No HTTP calls made
