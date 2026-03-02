from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from qdarchive_seeding.app.config_models import PipelineConfig
from qdarchive_seeding.core.entities import DatasetRecord
from qdarchive_seeding.infra.extractors.generic_rest import (
    GenericRestExtractor,
    GenericRestOptions,
)
from qdarchive_seeding.infra.extractors.html_scraper import (
    HtmlScraperExtractor,
    HtmlScraperOptions,
)
from qdarchive_seeding.infra.extractors.static_list import (
    StaticListExtractor,
    StaticListOptions,
)
from qdarchive_seeding.infra.extractors.syracuse_qdr import (
    SyracuseQdrExtractor,
    SyracuseQdrOptions,
)
from qdarchive_seeding.infra.extractors.zenodo import ZenodoExtractor, ZenodoOptions
from qdarchive_seeding.infra.http.auth import NoAuth

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "fixtures"


# ---------------------------------------------------------------------------
# Helpers: FakeResponse, FakeHttpClient, FakeRunContext
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FakeResponse:
    """Minimal httpx.Response stand-in with .json(), .text, and .raise_for_status()."""

    _json: Any
    text: str = ""

    def json(self) -> Any:
        return self._json

    def raise_for_status(self) -> None:
        pass


class FakeHttpClient:
    """HttpClient mock that returns pre-configured responses in order.

    When all responses are exhausted it returns *empty_response* forever
    (useful for stopping pagination).
    """

    def __init__(self, responses: list[Any], *, empty_response: Any | None = None) -> None:
        self._responses = list(responses)
        self._empty = empty_response or {}
        self.calls: list[dict[str, Any]] = []

    def get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> FakeResponse:
        self.calls.append({"url": url, "headers": headers, "params": params})
        payload = self._responses.pop(0) if self._responses else self._empty
        return FakeResponse(_json=payload)


@dataclass(slots=True)
class FakeRunContext:
    """Minimal RunContext implementation for testing."""

    run_id: str = "test-run-001"
    pipeline_id: str = "test_pipeline"
    config: PipelineConfig = field(default=None)  # type: ignore[assignment]
    cancelled: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _make_config(overrides: dict[str, Any] | None = None) -> PipelineConfig:
    """Build a PipelineConfig from the minimal template with optional overrides."""
    base: dict[str, Any] = {
        "pipeline": {"id": "test_pipeline", "run_mode": "incremental"},
        "source": {
            "name": "test",
            "type": "rest_api",
            "base_url": "https://api.example.com",
        },
        "auth": {"type": "none"},
        "extractor": {"name": "static_list_extractor", "options": {"records": []}},
        "transforms": [],
        "storage": {
            "downloads_root": "./downloads",
            "layout": "{source_name}/{dataset_slug}/",
        },
        "sink": {"type": "sqlite", "options": {"path": "./test.sqlite"}},
        "logging": {"level": "WARNING"},
    }
    if overrides:
        for key, value in overrides.items():
            if isinstance(value, dict) and key in base and isinstance(base[key], dict):
                base[key] = {**base[key], **value}
            else:
                base[key] = value
    return PipelineConfig.model_validate(base)


# ===================================================================
# ZenodoExtractor tests
# ===================================================================


class TestZenodoExtractor:
    """Tests for ZenodoExtractor using the fixture at tests/fixtures/zenodo_response.json."""

    @pytest.fixture()
    def zenodo_payload(self) -> dict[str, Any]:
        return json.loads((FIXTURES_DIR / "zenodo_response.json").read_text())

    def test_extracts_two_records_with_correct_fields(self, zenodo_payload: dict[str, Any]) -> None:
        """ZenodoExtractor should yield 2 DatasetRecords with the right metadata and assets."""
        empty_page: dict[str, Any] = {"hits": {"hits": [], "total": 0}}
        http_client = FakeHttpClient([zenodo_payload, empty_page])

        config = _make_config(
            {
                "source": {
                    "name": "zenodo",
                    "type": "rest_api",
                    "base_url": "https://zenodo.org/api",
                    "endpoints": {"search": "/records"},
                },
            }
        )
        ctx = FakeRunContext(config=config)
        extractor = ZenodoExtractor(
            http_client=http_client,
            auth=NoAuth(),
            options=ZenodoOptions(),
        )

        records = list(extractor.extract(ctx))

        assert len(records) == 2

        # --- First record ---
        r0 = records[0]
        assert isinstance(r0, DatasetRecord)
        assert r0.source_name == "zenodo"
        assert r0.source_dataset_id == "12345"
        assert r0.source_url == "https://zenodo.org/api/records/12345"
        assert r0.title == "Test QDP Dataset"
        assert r0.description == "A qualitative data package for testing"
        assert r0.doi == "10.5281/zenodo.12345"
        assert r0.license == "CC-BY-4.0"
        assert r0.year == 2024
        assert r0.owner_name == "Jane Doe"
        assert len(r0.assets) == 2
        assert r0.assets[0].asset_url == "https://zenodo.org/api/files/abc/dataset.qdpx"
        assert r0.assets[0].local_filename == "dataset.qdpx"
        assert r0.assets[1].asset_url == "https://zenodo.org/api/files/abc/readme.pdf"
        assert r0.assets[1].local_filename == "readme.pdf"

        # --- Second record ---
        r1 = records[1]
        assert r1.source_dataset_id == "67890"
        assert r1.title == "Another QDP Study"
        assert r1.doi == "10.5281/zenodo.67890"
        assert r1.license == "MIT"
        assert r1.year == 2023
        assert r1.owner_name == "John Smith"
        assert len(r1.assets) == 1
        assert r1.assets[0].local_filename == "data.zip"

    def test_empty_hits_stops_pagination(self) -> None:
        """When the first page returns no hits, the extractor should yield nothing."""
        empty_page: dict[str, Any] = {"hits": {"hits": [], "total": 0}}
        http_client = FakeHttpClient([empty_page])

        config = _make_config(
            {
                "source": {
                    "name": "zenodo",
                    "type": "rest_api",
                    "base_url": "https://zenodo.org/api",
                    "endpoints": {"search": "/records"},
                },
            }
        )
        ctx = FakeRunContext(config=config)
        extractor = ZenodoExtractor(
            http_client=http_client,
            auth=NoAuth(),
            options=ZenodoOptions(),
        )

        records = list(extractor.extract(ctx))

        assert records == []
        assert len(http_client.calls) == 1

    def test_max_pages_limit_stops(self) -> None:
        page1: dict[str, Any] = {"hits": {"hits": [{"id": "1", "metadata": {}, "files": []}]}}
        page2: dict[str, Any] = {"hits": {"hits": [{"id": "2", "metadata": {}, "files": []}]}}
        http_client = FakeHttpClient([page1, page2])

        config = _make_config(
            {
                "source": {
                    "name": "zenodo",
                    "type": "rest_api",
                    "base_url": "https://zenodo.org/api",
                    "endpoints": {"search": "/records"},
                }
            }
        )
        ctx = FakeRunContext(config=config)
        extractor = ZenodoExtractor(
            http_client=http_client,
            auth=NoAuth(),
            options=ZenodoOptions(max_pages=1),
        )

        records = list(extractor.extract(ctx))

        assert len(records) == 1
        assert records[0].source_dataset_id == "1"


# ===================================================================
# StaticListExtractor tests
# ===================================================================


class TestStaticListExtractor:
    """Tests for StaticListExtractor using inline record data."""

    def test_extracts_records_from_inline_data(self, minimal_config: PipelineConfig) -> None:
        """StaticListExtractor should yield one DatasetRecord per inline dict."""
        inline_records: list[dict[str, Any]] = [
            {
                "id": "rec-1",
                "source_url": "https://example.com/datasets/1",
                "title": "Inline Dataset One",
                "description": "First inline record",
                "assets": [
                    "https://example.com/files/a.qdpx",
                    "https://example.com/files/b.pdf",
                ],
            },
            {
                "id": "rec-2",
                "source_url": "https://example.com/datasets/2",
                "title": "Inline Dataset Two",
                "description": "Second inline record",
                "assets": [],
            },
        ]

        ctx = FakeRunContext(config=minimal_config)
        extractor = StaticListExtractor(
            options=StaticListOptions(records=inline_records),
        )

        records = extractor.extract(ctx)

        assert len(records) == 2

        r0 = records[0]
        assert r0.source_name == "test"
        assert r0.source_dataset_id == "rec-1"
        assert r0.source_url == "https://example.com/datasets/1"
        assert r0.title == "Inline Dataset One"
        assert r0.description == "First inline record"
        assert len(r0.assets) == 2
        assert r0.assets[0].asset_url == "https://example.com/files/a.qdpx"
        assert r0.assets[1].asset_url == "https://example.com/files/b.pdf"

        r1 = records[1]
        assert r1.source_dataset_id == "rec-2"
        assert r1.title == "Inline Dataset Two"
        assert r1.assets == []

    def test_empty_list_yields_no_records(self, minimal_config: PipelineConfig) -> None:
        """StaticListExtractor with an empty records list should return nothing."""
        ctx = FakeRunContext(config=minimal_config)
        extractor = StaticListExtractor(options=StaticListOptions(records=[]))

        records = extractor.extract(ctx)

        assert records == []


# ===================================================================
# GenericRestExtractor tests
# ===================================================================


class TestGenericRestExtractor:
    """Tests for GenericRestExtractor using a mock REST API response."""

    def test_extracts_records_from_rest_response(self) -> None:
        """GenericRestExtractor should yield records from a paginated REST response."""
        page1: dict[str, Any] = {
            "items": [
                {
                    "id": "api-1",
                    "url": "https://api.example.com/items/1",
                    "title": "REST Item One",
                    "description": "First REST item",
                    "assets": ["https://api.example.com/files/x.csv"],
                },
                {
                    "id": "api-2",
                    "url": "https://api.example.com/items/2",
                    "title": "REST Item Two",
                    "description": "Second REST item",
                    "assets": [],
                },
            ]
        }
        page2_empty: dict[str, Any] = {"items": []}

        http_client = FakeHttpClient([page1, page2_empty])
        config = _make_config(
            {
                "source": {
                    "name": "rest_source",
                    "type": "rest_api",
                    "base_url": "https://api.example.com",
                    "endpoints": {"search": "/items"},
                },
            }
        )
        ctx = FakeRunContext(config=config)
        extractor = GenericRestExtractor(
            http_client=http_client,
            auth=NoAuth(),
            options=GenericRestOptions(records_path="items"),
        )

        records = list(extractor.extract(ctx))

        assert len(records) == 2

        r0 = records[0]
        assert isinstance(r0, DatasetRecord)
        assert r0.source_name == "rest_source"
        assert r0.source_dataset_id == "api-1"
        assert r0.source_url == "https://api.example.com/items/1"
        assert r0.title == "REST Item One"
        assert r0.description == "First REST item"
        assert len(r0.assets) == 1
        assert r0.assets[0].asset_url == "https://api.example.com/files/x.csv"

        r1 = records[1]
        assert r1.source_dataset_id == "api-2"
        assert r1.title == "REST Item Two"
        assert r1.assets == []

    def test_pages_correctly_until_empty(self) -> None:
        """GenericRestExtractor should stop paginating when the items list is empty."""
        page1: dict[str, Any] = {
            "items": [
                {"id": "p1", "title": "Page 1 Item"},
            ]
        }
        page2: dict[str, Any] = {
            "items": [
                {"id": "p2", "title": "Page 2 Item"},
            ]
        }
        page3_empty: dict[str, Any] = {"items": []}

        http_client = FakeHttpClient([page1, page2, page3_empty])
        config = _make_config(
            {
                "source": {
                    "name": "paged_source",
                    "type": "rest_api",
                    "base_url": "https://api.example.com",
                    "endpoints": {"search": "/data"},
                },
            }
        )
        ctx = FakeRunContext(config=config)
        extractor = GenericRestExtractor(
            http_client=http_client,
            auth=NoAuth(),
            options=GenericRestOptions(records_path="items"),
        )

        records = list(extractor.extract(ctx))

        assert len(records) == 2
        assert records[0].source_dataset_id == "p1"
        assert records[1].source_dataset_id == "p2"
        # 3 HTTP calls: page 1, page 2, page 3 (empty → stop)
        assert len(http_client.calls) == 3

    def test_nested_records_path(self) -> None:
        """GenericRestExtractor should navigate a dotted records_path like 'data.results'."""
        page1: dict[str, Any] = {
            "data": {
                "results": [
                    {"id": "nested-1", "title": "Nested Item"},
                ]
            }
        }
        page2_empty: dict[str, Any] = {"data": {"results": []}}

        http_client = FakeHttpClient([page1, page2_empty])
        config = _make_config(
            {
                "source": {
                    "name": "nested_source",
                    "type": "rest_api",
                    "base_url": "https://api.example.com",
                    "endpoints": {"search": "/nested"},
                },
            }
        )
        ctx = FakeRunContext(config=config)
        extractor = GenericRestExtractor(
            http_client=http_client,
            auth=NoAuth(),
            options=GenericRestOptions(records_path="data.results"),
        )

        records = list(extractor.extract(ctx))

        assert len(records) == 1
        assert records[0].source_dataset_id == "nested-1"
        assert records[0].title == "Nested Item"

    def test_offset_pagination_type(self) -> None:
        page1: dict[str, Any] = {"items": [{"id": "1", "title": "One"}]}
        page2_empty: dict[str, Any] = {"items": []}

        http_client = FakeHttpClient([page1, page2_empty])
        config = _make_config(
            {
                "source": {
                    "name": "offset_source",
                    "type": "rest_api",
                    "base_url": "https://api.example.com",
                    "endpoints": {"search": "/items"},
                    "pagination": {"type": "offset", "size_param": "limit"},
                }
            }
        )
        ctx = FakeRunContext(config=config)
        extractor = GenericRestExtractor(
            http_client=http_client,
            auth=NoAuth(),
            options=GenericRestOptions(records_path="items"),
        )

        records = list(extractor.extract(ctx))

        assert len(records) == 1
        assert http_client.calls[0]["params"].get("offset") == 0

    def test_cursor_pagination_type(self) -> None:
        page1: dict[str, Any] = {"items": [{"id": "1", "title": "One"}]}

        http_client = FakeHttpClient([page1])
        config = _make_config(
            {
                "source": {
                    "name": "cursor_source",
                    "type": "rest_api",
                    "base_url": "https://api.example.com",
                    "endpoints": {"search": "/items"},
                    "pagination": {"type": "cursor", "cursor_param": "cursor"},
                }
            }
        )
        ctx = FakeRunContext(config=config)
        extractor = GenericRestExtractor(
            http_client=http_client,
            auth=NoAuth(),
            options=GenericRestOptions(records_path="items"),
        )

        records = list(extractor.extract(ctx))

        assert len(records) == 1
        assert "cursor" not in http_client.calls[0]["params"]

    def test_max_pages_limits_extraction(self) -> None:
        page1: dict[str, Any] = {"items": [{"id": "1"}]}
        page2: dict[str, Any] = {"items": [{"id": "2"}]}

        http_client = FakeHttpClient([page1, page2])
        config = _make_config(
            {
                "source": {
                    "name": "limited_source",
                    "type": "rest_api",
                    "base_url": "https://api.example.com",
                    "endpoints": {"search": "/items"},
                }
            }
        )
        ctx = FakeRunContext(config=config)
        extractor = GenericRestExtractor(
            http_client=http_client,
            auth=NoAuth(),
            options=GenericRestOptions(records_path="items", max_pages=1),
        )

        records = list(extractor.extract(ctx))

        assert len(records) == 1
        assert records[0].source_dataset_id == "1"

    def test_items_not_list_stops(self) -> None:
        page1: dict[str, Any] = {"items": {"id": "1"}}

        http_client = FakeHttpClient([page1])
        config = _make_config(
            {
                "source": {
                    "name": "bad_items",
                    "type": "rest_api",
                    "base_url": "https://api.example.com",
                    "endpoints": {"search": "/items"},
                }
            }
        )
        ctx = FakeRunContext(config=config)
        extractor = GenericRestExtractor(
            http_client=http_client,
            auth=NoAuth(),
            options=GenericRestOptions(records_path="items"),
        )

        records = list(extractor.extract(ctx))

        assert records == []

    def test_skips_non_dict_items(self) -> None:
        page1: dict[str, Any] = {"items": ["bad", {"id": "ok", "title": "Good"}]}
        page2_empty: dict[str, Any] = {"items": []}

        http_client = FakeHttpClient([page1, page2_empty])
        config = _make_config(
            {
                "source": {
                    "name": "mixed_items",
                    "type": "rest_api",
                    "base_url": "https://api.example.com",
                    "endpoints": {"search": "/items"},
                }
            }
        )
        ctx = FakeRunContext(config=config)
        extractor = GenericRestExtractor(
            http_client=http_client,
            auth=NoAuth(),
            options=GenericRestOptions(records_path="items"),
        )

        records = list(extractor.extract(ctx))

        assert len(records) == 1
        assert records[0].source_dataset_id == "ok"


class TestZenodoExtractorEdges:
    def test_include_files_false(self) -> None:
        payload: dict[str, Any] = {
            "hits": {"hits": [{"id": "1", "metadata": {}, "files": [{"key": "f"}]}]}
        }
        http_client = FakeHttpClient([payload])
        config = _make_config(
            {
                "source": {
                    "name": "zenodo",
                    "type": "rest_api",
                    "base_url": "https://zenodo.org/api",
                    "endpoints": {"search": "/records"},
                }
            }
        )
        ctx = FakeRunContext(config=config)
        extractor = ZenodoExtractor(
            http_client=http_client,
            auth=NoAuth(),
            options=ZenodoOptions(include_files=False),
        )

        records = list(extractor.extract(ctx))

        assert len(records) == 1
        assert records[0].assets == []

    def test_hits_not_list_stops(self) -> None:
        payload: dict[str, Any] = {"hits": {"hits": {"id": "1"}}}
        http_client = FakeHttpClient([payload])
        config = _make_config(
            {
                "source": {
                    "name": "zenodo",
                    "type": "rest_api",
                    "base_url": "https://zenodo.org/api",
                    "endpoints": {"search": "/records"},
                }
            }
        )
        ctx = FakeRunContext(config=config)
        extractor = ZenodoExtractor(
            http_client=http_client,
            auth=NoAuth(),
            options=ZenodoOptions(),
        )

        records = list(extractor.extract(ctx))

        assert records == []

    def test_license_and_year_parsing(self) -> None:
        payload: dict[str, Any] = {
            "hits": {
                "hits": [
                    {
                        "id": "1",
                        "metadata": {
                            "license": {"id": "cc-by"},
                            "publication_date": "bad-date",
                        },
                        "files": [],
                    }
                ]
            }
        }
        http_client = FakeHttpClient([payload])
        config = _make_config(
            {
                "source": {
                    "name": "zenodo",
                    "type": "rest_api",
                    "base_url": "https://zenodo.org/api",
                    "endpoints": {"search": "/records"},
                }
            }
        )
        ctx = FakeRunContext(config=config)
        extractor = ZenodoExtractor(
            http_client=http_client,
            auth=NoAuth(),
            options=ZenodoOptions(),
        )

        records = list(extractor.extract(ctx))

        assert records[0].license == "cc-by"
        assert records[0].year is None


class TestHtmlScraperExtractor:
    def test_extracts_records_from_html(self) -> None:
        html = (
            "<div class='item'>"
            "<a class='link' href='https://example.com/ds1'>Link</a>"
            "<h2 class='title'>Title 1</h2>"
            "<p class='desc'>Desc 1</p>"
            "<a class='asset' href='https://example.com/file1.qdpx'>file</a>"
            "</div>"
        )

        class HtmlClient:
            def get(self, _url: str, *, headers=None, params=None, timeout=None):
                return FakeResponse(_json={}, text=html)

        config = _make_config(
            {
                "source": {
                    "name": "html",
                    "type": "html",
                    "base_url": "https://example.com",
                    "endpoints": {"search": "/list"},
                }
            }
        )
        ctx = FakeRunContext(config=config)
        extractor = HtmlScraperExtractor(
            http_client=HtmlClient(),
            options=HtmlScraperOptions(
                list_selector=".item",
                title_selector=".title",
                link_selector=".link",
                description_selector=".desc",
                asset_selector=".asset",
            ),
        )

        records = extractor.extract(ctx)

        assert len(records) == 1
        assert records[0].title == "Title 1"
        assert records[0].description == "Desc 1"
        assert records[0].assets[0].asset_url == "https://example.com/file1.qdpx"

    def test_respects_max_items_and_skips_missing_nodes(self) -> None:
        html = (
            "<div class='item'><h2 class='title'>A</h2></div>"
            "<div class='item'>"
            "<a class='link' href='https://example.com/ds1'>Link</a>"
            "<h2 class='title'>Title 1</h2>"
            "</div>"
            "<div class='item'>"
            "<a class='link' href='https://example.com/ds2'>Link</a>"
            "<h2 class='title'>Title 2</h2>"
            "</div>"
        )

        class HtmlClient:
            def get(self, _url: str, *, headers=None, params=None, timeout=None):
                return FakeResponse(_json={}, text=html)

        config = _make_config(
            {
                "source": {
                    "name": "html",
                    "type": "html",
                    "base_url": "https://example.com",
                    "endpoints": {"search": "/list"},
                }
            }
        )
        ctx = FakeRunContext(config=config)
        extractor = HtmlScraperExtractor(
            http_client=HtmlClient(),
            options=HtmlScraperOptions(
                list_selector=".item",
                title_selector=".title",
                link_selector=".link",
                max_items=1,
            ),
        )

        records = extractor.extract(ctx)

        assert len(records) == 1
        assert records[0].title == "Title 1"


class TestSyracuseQdrExtractor:
    def test_extracts_datasets_and_files(self) -> None:
        search_payload: dict[str, Any] = {
            "data": {
                "items": [
                    {
                        "global_id": "doi:10.123/abc",
                        "name": "Dataset 1",
                        "description": "Desc",
                        "published_at": "2024-01-02T00:00:00Z",
                        "authors": ["Smith, Jane"],
                    }
                ],
                "total_count": 1,
            }
        }
        files_payload: dict[str, Any] = {
            "data": {
                "latestVersion": {
                    "license": {"name": "CC0"},
                    "files": [{"dataFile": {"id": 1, "filename": "file.txt", "filesize": 10}}],
                }
            }
        }

        http_client = FakeHttpClient([search_payload, files_payload])
        config = _make_config(
            {
                "source": {
                    "name": "syracuse",
                    "type": "rest_api",
                    "base_url": "https://example.com/api",
                    "endpoints": {"search": "/search", "dataset": "/datasets/:persistentId/"},
                }
            }
        )
        ctx = FakeRunContext(config=config)
        extractor = SyracuseQdrExtractor(
            http_client=http_client,
            auth=NoAuth(),
            options=SyracuseQdrOptions(max_datasets=10),
        )

        records = list(extractor.extract(ctx))

        assert len(records) == 1
        assert records[0].license == "CC0"
        assert records[0].year == 2024
        assert records[0].owner_name == "Smith, Jane"
        assert records[0].assets[0].local_filename == "file.txt"

    def test_skips_missing_global_id_and_file_errors(self) -> None:
        search_payload: dict[str, Any] = {
            "data": {
                "items": [
                    {"name": "Missing ID"},
                    {"global_id": "doi:10.999/err", "name": "Bad Files"},
                ],
                "total_count": 2,
            }
        }

        class ErrorHttpClient(FakeHttpClient):
            def get(
                self,
                url: str,
                *,
                headers: dict[str, str] | None = None,
                params: dict[str, Any] | None = None,
                timeout: float | None = None,
            ) -> FakeResponse:  # type: ignore[override]
                if "datasets" in url:
                    raise RuntimeError("fail")
                return super().get(url, headers=headers, params=params, timeout=timeout)

        http_client = ErrorHttpClient([search_payload])
        config = _make_config(
            {
                "source": {
                    "name": "syracuse",
                    "type": "rest_api",
                    "base_url": "https://example.com/api",
                    "endpoints": {"search": "/search", "dataset": "/datasets/:persistentId/"},
                }
            }
        )
        ctx = FakeRunContext(config=config)
        extractor = SyracuseQdrExtractor(
            http_client=http_client,
            auth=NoAuth(),
            options=SyracuseQdrOptions(max_datasets=10),
        )

        records = list(extractor.extract(ctx))

        assert records == []

    def test_stops_when_max_datasets_reached(self) -> None:
        search_payload: dict[str, Any] = {
            "data": {
                "items": [{"global_id": "doi:10.1/one", "name": "One"}],
                "total_count": 1,
            }
        }
        http_client = FakeHttpClient([search_payload])
        config = _make_config(
            {
                "source": {
                    "name": "syracuse",
                    "type": "rest_api",
                    "base_url": "https://example.com/api",
                    "endpoints": {"search": "/search", "dataset": "/datasets/:persistentId/"},
                }
            }
        )
        ctx = FakeRunContext(config=config)
        extractor = SyracuseQdrExtractor(
            http_client=http_client,
            auth=NoAuth(),
            options=SyracuseQdrOptions(max_datasets=-1),
        )

        records = list(extractor.extract(ctx))

        assert records == []
        assert http_client.calls == []

    def test_breaks_when_items_invalid_or_empty(self) -> None:
        search_payload: dict[str, Any] = {"data": {"items": {}, "total_count": 0}}
        http_client = FakeHttpClient([search_payload])
        config = _make_config(
            {
                "source": {
                    "name": "syracuse",
                    "type": "rest_api",
                    "base_url": "https://example.com/api",
                    "endpoints": {"search": "/search", "dataset": "/datasets/:persistentId/"},
                }
            }
        )
        ctx = FakeRunContext(config=config)
        extractor = SyracuseQdrExtractor(
            http_client=http_client,
            auth=NoAuth(),
            options=SyracuseQdrOptions(max_datasets=10),
        )

        records = list(extractor.extract(ctx))

        assert records == []

    def test_respects_effective_max_inside_items(self) -> None:
        search_payload: dict[str, Any] = {
            "data": {
                "items": [
                    {"global_id": "doi:10.1/one", "name": "One"},
                    {"global_id": "doi:10.1/two", "name": "Two"},
                ],
                "total_count": 2,
            }
        }
        files_payload: dict[str, Any] = {"data": {"latestVersion": {"files": []}}}
        http_client = FakeHttpClient([search_payload, files_payload])
        config = _make_config(
            {
                "source": {
                    "name": "syracuse",
                    "type": "rest_api",
                    "base_url": "https://example.com/api",
                    "endpoints": {"search": "/search", "dataset": "/datasets/:persistentId/"},
                }
            }
        )
        ctx = FakeRunContext(config=config)
        extractor = SyracuseQdrExtractor(
            http_client=http_client,
            auth=NoAuth(),
            options=SyracuseQdrOptions(max_datasets=1),
        )

        records = list(extractor.extract(ctx))

        assert len(records) == 1

    def test_license_string_and_invalid_year_and_no_authors(self) -> None:
        search_payload: dict[str, Any] = {
            "data": {
                "items": [{"global_id": "doi:10.1/one", "name": "One", "published_at": "bad"}],
                "total_count": 1,
            }
        }
        files_payload: dict[str, Any] = {"data": {"latestVersion": {"license": "MIT", "files": []}}}
        http_client = FakeHttpClient([search_payload, files_payload])
        config = _make_config(
            {
                "source": {
                    "name": "syracuse",
                    "type": "rest_api",
                    "base_url": "https://example.com/api",
                    "endpoints": {"search": "/search", "dataset": "/datasets/:persistentId/"},
                }
            }
        )
        ctx = FakeRunContext(config=config)
        extractor = SyracuseQdrExtractor(
            http_client=http_client,
            auth=NoAuth(),
            options=SyracuseQdrOptions(max_datasets=10),
        )

        records = list(extractor.extract(ctx))

        assert records[0].license == "MIT"
        assert records[0].year is None
        assert records[0].owner_name is None
