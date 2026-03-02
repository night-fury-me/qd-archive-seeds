from __future__ import annotations

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.infra.transforms.base import BaseTransform, TransformChain
from qdarchive_seeding.infra.transforms.classify_qda_files import ClassifyQdaFiles
from qdarchive_seeding.infra.transforms.deduplicate_assets import DeduplicateAssets
from qdarchive_seeding.infra.transforms.filter_by_extensions import FilterByExtensions
from qdarchive_seeding.infra.transforms.infer_filetypes import InferFileTypes
from qdarchive_seeding.infra.transforms.normalize_fields import NormalizeFields
from qdarchive_seeding.infra.transforms.slugify_dataset import SlugifyDataset
from qdarchive_seeding.infra.transforms.validate_required import ValidateRequiredFields


def _make_record(**kwargs: object) -> DatasetRecord:
    defaults = {
        "source_name": "test",
        "source_dataset_id": "1",
        "source_url": "https://example.com",
    }
    defaults.update(kwargs)
    return DatasetRecord(**defaults)  # type: ignore[arg-type]


def test_validate_required_passes() -> None:
    t = ValidateRequiredFields(name="v", required_fields=["source_url"])
    record = _make_record(source_url="https://example.com")
    assert t.apply(record) is not None


def test_base_transform_apply_raises() -> None:
    t = BaseTransform(name="base")
    try:
        t.apply(_make_record())
    except NotImplementedError:
        assert True
    else:
        raise AssertionError("Expected NotImplementedError")


def test_validate_required_filters_missing() -> None:
    t = ValidateRequiredFields(name="v", required_fields=["asset_url"])
    record = _make_record()
    assert t.apply(record) is None


def test_validate_required_has_assets_passes() -> None:
    t = ValidateRequiredFields(name="v", required_fields=["has_assets"])
    record = _make_record(assets=[AssetRecord(asset_url="https://example.com/f.pdf")])
    assert t.apply(record) is not None


def test_validate_required_has_assets_filters_empty() -> None:
    t = ValidateRequiredFields(name="v", required_fields=["has_assets"])
    record = _make_record()
    assert t.apply(record) is None


def test_validate_required_assets_dot_field() -> None:
    t = ValidateRequiredFields(name="v", required_fields=["assets.asset_url"])
    record = _make_record(assets=[AssetRecord(asset_url="https://example.com/f.pdf")])
    assert t.apply(record) is not None


def test_validate_required_assets_dot_field_filters_empty_url() -> None:
    t = ValidateRequiredFields(name="v", required_fields=["assets.asset_url"])
    record = _make_record(assets=[AssetRecord(asset_url="")])
    assert t.apply(record) is None


def test_normalize_fields_from_raw() -> None:
    t = NormalizeFields(name="n")
    record = _make_record(raw={"owner": {"name": "Alice", "email": "a@b.com"}})
    result = t.apply(record)
    assert result is not None
    assert result.owner_name == "Alice"
    assert result.owner_email == "a@b.com"


def test_normalize_fields_owner_string() -> None:
    t = NormalizeFields(name="n")
    record = _make_record(raw={"owner": "Jane"})
    result = t.apply(record)
    assert result is not None
    assert result.owner_name == "Jane"


def test_infer_filetypes() -> None:
    t = InferFileTypes(name="i")
    record = _make_record(
        assets=[
            AssetRecord(asset_url="https://example.com/file.qdpx"),
            AssetRecord(asset_url="https://example.com/file.zip"),
            AssetRecord(asset_url="https://example.com/file.pdf"),
            AssetRecord(asset_url="https://example.com/file.xyz"),
        ]
    )
    result = t.apply(record)
    assert result is not None
    assert result.assets[0].asset_type == "qdpx"
    assert result.assets[1].asset_type == "archive"
    assert result.assets[2].asset_type == "document"
    assert result.assets[3].asset_type == "unknown"


def test_infer_filetypes_skips_existing_type() -> None:
    t = InferFileTypes(name="i")
    record = _make_record(
        assets=[
            AssetRecord(asset_url="https://example.com/file.qdpx", asset_type="custom"),
        ]
    )
    result = t.apply(record)
    assert result is not None
    assert result.assets[0].asset_type == "custom"


def test_deduplicate_assets() -> None:
    t = DeduplicateAssets(name="d")
    record = _make_record(
        assets=[
            AssetRecord(asset_url="https://example.com/a.pdf"),
            AssetRecord(asset_url="https://example.com/b.pdf"),
            AssetRecord(asset_url="https://example.com/a.pdf"),
        ]
    )
    result = t.apply(record)
    assert result is not None
    assert len(result.assets) == 2


def test_slugify_dataset() -> None:
    t = SlugifyDataset(name="s")
    record = _make_record(title="Hello World! Test (2024)")
    result = t.apply(record)
    assert result is not None
    assert result.raw is not None
    slug = result.raw["dataset_slug"]
    assert " " not in slug
    assert slug == "hello-world-test-2024"


def test_slugify_dataset_falls_back_to_source_id() -> None:
    t = SlugifyDataset(name="s")
    record = _make_record(source_dataset_id="ID 123")
    record.title = None
    result = t.apply(record)
    assert result is not None
    assert result.raw is not None
    assert result.raw["dataset_slug"] == "id-123"


def test_slugify_dataset_default_when_missing_all() -> None:
    t = SlugifyDataset(name="s")
    record = _make_record(source_dataset_id=None, source_url="https://example.com")
    record.title = None
    result = t.apply(record)
    assert result is not None
    assert result.raw is not None
    assert result.raw["dataset_slug"] == "dataset"


def test_transform_chain_short_circuits() -> None:
    v = ValidateRequiredFields(name="v", required_fields=["nonexistent_field"])
    s = SlugifyDataset(name="s")
    chain = TransformChain(transforms=[v, s])
    records = [_make_record()]
    result = chain.run(records)
    assert len(result) == 0


def test_transform_chain_full_pipeline() -> None:
    chain = TransformChain(
        transforms=[
            ValidateRequiredFields(name="v", required_fields=["source_url"]),
            NormalizeFields(name="n"),
            InferFileTypes(name="i"),
            DeduplicateAssets(name="d"),
            SlugifyDataset(name="s"),
        ]
    )
    records = [
        _make_record(
            title="Test",
            assets=[
                AssetRecord(asset_url="https://example.com/a.qdpx"),
                AssetRecord(asset_url="https://example.com/a.qdpx"),
            ],
        ),
    ]
    result = chain.run(records)
    assert len(result) == 1
    assert len(result[0].assets) == 1
    assert result[0].raw is not None
    assert result[0].raw["dataset_slug"] == "test"


def test_classify_qda_files_analysis_data() -> None:
    t = ClassifyQdaFiles(name="c")
    record = _make_record(
        assets=[
            AssetRecord(asset_url="https://example.com/project.qdpx"),
            AssetRecord(asset_url="https://example.com/project.nvpx"),
            AssetRecord(asset_url="https://example.com/project.atlasproj"),
        ]
    )
    result = t.apply(record)
    assert result is not None
    assert all(a.asset_type == "analysis_data" for a in result.assets)


def test_classify_qda_files_uses_local_filename() -> None:
    t = ClassifyQdaFiles(name="c")
    record = _make_record(
        assets=[
            AssetRecord(asset_url="https://example.com/unknown", local_filename="file.qdpx"),
        ]
    )
    result = t.apply(record)
    assert result is not None
    assert result.assets[0].asset_type == "analysis_data"


def test_classify_qda_files_primary_data() -> None:
    t = ClassifyQdaFiles(name="c")
    record = _make_record(
        assets=[
            AssetRecord(asset_url="https://example.com/doc.pdf"),
            AssetRecord(asset_url="https://example.com/audio.mp3"),
            AssetRecord(asset_url="https://example.com/data.csv"),
        ]
    )
    result = t.apply(record)
    assert result is not None
    assert all(a.asset_type == "primary_data" for a in result.assets)


def test_classify_qda_files_additional_data() -> None:
    t = ClassifyQdaFiles(name="c")
    record = _make_record(
        assets=[
            AssetRecord(asset_url="https://example.com/archive.zip"),
            AssetRecord(asset_url="https://example.com/notes.md"),
            AssetRecord(asset_url="https://example.com/LICENSE"),
        ]
    )
    result = t.apply(record)
    assert result is not None
    assert all(a.asset_type == "additional_data" for a in result.assets)


def test_filter_by_extensions_keeps_matching_assets() -> None:
    t = FilterByExtensions(name="f", categories=["analysis_data"], extra_extensions=[".zip"])
    record = _make_record(
        assets=[
            AssetRecord(asset_url="https://example.com/file.qdpx"),
            AssetRecord(asset_url="https://example.com/archive.zip"),
        ]
    )
    assert t.apply(record) is not None


def test_filter_by_extensions_drops_when_no_match() -> None:
    t = FilterByExtensions(name="f", categories=["analysis_data"], extra_extensions=[])
    record = _make_record(
        assets=[
            AssetRecord(asset_url="https://example.com/file.txt"),
        ]
    )
    assert t.apply(record) is None


def test_filter_by_extensions_primary_data_and_local_filename() -> None:
    t = FilterByExtensions(name="f", categories=["primary_data"], extra_extensions=[])
    record = _make_record(
        assets=[
            AssetRecord(asset_url="https://example.com/ignored", local_filename="file.pdf"),
        ]
    )
    assert t.apply(record) is not None
