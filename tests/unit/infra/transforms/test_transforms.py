from __future__ import annotations

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.infra.transforms._text_utils import normalize_text
from qdarchive_seeding.infra.transforms.base import BaseTransform, TransformChain
from qdarchive_seeding.infra.transforms.blocklist_extensions import (
    SCIENCE_BLOCKLIST_EXTENSIONS,
    BlocklistExtensions,
)
from qdarchive_seeding.infra.transforms.classify_qda_files import ClassifyQdaFiles
from qdarchive_seeding.infra.transforms.deduplicate_assets import DeduplicateAssets
from qdarchive_seeding.infra.transforms.extension_ratio_filter import ExtensionRatioFilter
from qdarchive_seeding.infra.transforms.filter_by_extensions import FilterByExtensions
from qdarchive_seeding.infra.transforms.infer_filetypes import InferFileTypes
from qdarchive_seeding.infra.transforms.metadata_relevance_filter import (
    MetadataRelevanceFilter,
)
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
            AssetRecord(asset_url="https://example.com/notes.md"),
            AssetRecord(asset_url="https://example.com/LICENSE"),
            AssetRecord(asset_url="https://example.com/codebook.xml"),
        ]
    )
    result = t.apply(record)
    assert result is not None
    assert all(a.asset_type == "additional_data" for a in result.assets)


def test_classify_qda_files_zip_is_primary_data() -> None:
    t = ClassifyQdaFiles(name="c")
    record = _make_record(assets=[AssetRecord(asset_url="https://example.com/archive.zip")])
    result = t.apply(record)
    assert result is not None
    assert result.assets[0].asset_type == "primary_data"


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


# ---------------------------------------------------------------------------
# BlocklistExtensions tests
# ---------------------------------------------------------------------------


def test_blocklist_drops_science_dataset() -> None:
    t = BlocklistExtensions(name="bl")
    record = _make_record(
        assets=[
            AssetRecord(asset_url="https://example.com/data.hdf5"),
            AssetRecord(asset_url="https://example.com/readme.pdf"),
        ]
    )
    assert t.apply(record) is None


def test_blocklist_keeps_clean_dataset() -> None:
    t = BlocklistExtensions(name="bl")
    record = _make_record(
        assets=[
            AssetRecord(asset_url="https://example.com/interview.docx"),
            AssetRecord(asset_url="https://example.com/codebook.pdf"),
        ]
    )
    assert t.apply(record) is not None


def test_blocklist_bypass_with_analysis_data() -> None:
    t = BlocklistExtensions(name="bl", bypass_with_analysis_data=True)
    record = _make_record(
        assets=[
            AssetRecord(asset_url="https://example.com/project.qdpx"),
            AssetRecord(asset_url="https://example.com/data.hdf5"),
        ]
    )
    # Dataset has a QDA file — should be kept despite .hdf5
    assert t.apply(record) is not None


def test_blocklist_no_bypass_drops_mixed() -> None:
    t = BlocklistExtensions(name="bl", bypass_with_analysis_data=False)
    record = _make_record(
        assets=[
            AssetRecord(asset_url="https://example.com/project.qdpx"),
            AssetRecord(asset_url="https://example.com/data.hdf5"),
        ]
    )
    assert t.apply(record) is None


def test_blocklist_custom_extra_and_exempt() -> None:
    custom_blocked = SCIENCE_BLOCKLIST_EXTENSIONS | frozenset({".custom"})
    custom_blocked = custom_blocked - frozenset({".hdf5"})
    t = BlocklistExtensions(name="bl", blocked_extensions=custom_blocked)
    # .hdf5 is now exempt — should pass
    record_hdf5 = _make_record(assets=[AssetRecord(asset_url="https://example.com/data.hdf5")])
    assert t.apply(record_hdf5) is not None
    # .custom is now blocked — should drop
    record_custom = _make_record(assets=[AssetRecord(asset_url="https://example.com/data.custom")])
    assert t.apply(record_custom) is None


# ---------------------------------------------------------------------------
# ExtensionRatioFilter tests
# ---------------------------------------------------------------------------


def test_ratio_filter_keeps_analysis_data() -> None:
    t = ExtensionRatioFilter(name="rf", min_primary_ratio=0.5)
    record = _make_record(
        assets=[
            AssetRecord(asset_url="https://example.com/project.qdpx", asset_type="analysis_data"),
            AssetRecord(asset_url="https://example.com/data.json", asset_type="additional_data"),
            AssetRecord(asset_url="https://example.com/meta.xml", asset_type="additional_data"),
        ]
    )
    # Has analysis_data — always kept
    assert t.apply(record) is not None


def test_ratio_filter_drops_low_ratio() -> None:
    t = ExtensionRatioFilter(name="rf", min_primary_ratio=0.5)
    record = _make_record(
        assets=[
            AssetRecord(asset_url="https://example.com/readme.pdf", asset_type="primary_data"),
            AssetRecord(asset_url="https://example.com/a.json", asset_type="additional_data"),
            AssetRecord(asset_url="https://example.com/b.xml", asset_type="additional_data"),
            AssetRecord(asset_url="https://example.com/c.yml", asset_type="additional_data"),
        ]
    )
    # 1/4 = 0.25 < 0.5 — should drop
    assert t.apply(record) is None


def test_ratio_filter_keeps_high_ratio() -> None:
    t = ExtensionRatioFilter(name="rf", min_primary_ratio=0.5)
    record = _make_record(
        assets=[
            AssetRecord(asset_url="https://example.com/a.docx", asset_type="primary_data"),
            AssetRecord(asset_url="https://example.com/b.docx", asset_type="primary_data"),
            AssetRecord(asset_url="https://example.com/c.json", asset_type="additional_data"),
        ]
    )
    # 2/3 = 0.67 >= 0.5 — should keep
    assert t.apply(record) is not None


def test_ratio_filter_boundary_at_half() -> None:
    t = ExtensionRatioFilter(name="rf", min_primary_ratio=0.5)
    record = _make_record(
        assets=[
            AssetRecord(asset_url="https://example.com/a.csv", asset_type="primary_data"),
            AssetRecord(asset_url="https://example.com/b.json", asset_type="additional_data"),
        ]
    )
    # 1/2 = 0.5 — at boundary, should keep (not strictly less than)
    assert t.apply(record) is not None


def test_ratio_filter_drops_empty_assets() -> None:
    t = ExtensionRatioFilter(name="rf", min_primary_ratio=0.5, bypass_with_analysis_data=False)
    record = _make_record(assets=[])
    assert t.apply(record) is None


# ---------------------------------------------------------------------------
# Text normalisation tests
# ---------------------------------------------------------------------------


def test_normalize_text_underscores() -> None:
    assert normalize_text("interview_transcript_data") == "interview transcript data"


def test_normalize_text_camelcase() -> None:
    assert normalize_text("interviewTranscriptData") == "interview transcript data"


def test_normalize_text_html_tags() -> None:
    result = normalize_text("<p>Interview <b>transcript</b> data</p>")
    assert result == "interview transcript data"


def test_normalize_text_special_chars() -> None:
    result = normalize_text("crystal-structure (v2.1) — data & results")
    assert "crystal structure" in result
    assert "data" in result
    assert "&" not in result


def test_normalize_text_empty() -> None:
    assert normalize_text("") == ""


# ---------------------------------------------------------------------------
# MetadataRelevanceFilter tests (keyword stage only — no model loading)
# ---------------------------------------------------------------------------


def test_relevance_keyword_keeps_qualitative() -> None:
    t = MetadataRelevanceFilter(
        name="mrf",
        keyword_keep_threshold=8.0,
        keyword_drop_threshold=-4.0,
        bypass_with_analysis_data=False,
    )
    record = _make_record(
        title="Semi-structured interview transcripts with healthcare workers",
        description="Thematic analysis of qualitative interview data",
        assets=[AssetRecord(asset_url="https://example.com/data.docx", asset_type="primary_data")],
    )
    assert t.apply(record) is not None


def test_relevance_keyword_drops_science() -> None:
    t = MetadataRelevanceFilter(
        name="mrf",
        keyword_keep_threshold=8.0,
        keyword_drop_threshold=-4.0,
        bypass_with_analysis_data=False,
    )
    record = _make_record(
        title="Genome-wide association study of cardiovascular disease",
        description="DNA sequencing and molecular dynamics analysis",
        assets=[AssetRecord(asset_url="https://example.com/data.csv", asset_type="primary_data")],
    )
    # "genome" (-8) + "dna sequenc" (-6) + "molecular dynamics" (-8) = -22 <= -4
    assert t.apply(record) is None


def test_relevance_bypass_with_analysis_data() -> None:
    t = MetadataRelevanceFilter(
        name="mrf",
        keyword_keep_threshold=8.0,
        keyword_drop_threshold=-4.0,
        bypass_with_analysis_data=True,
    )
    record = _make_record(
        title="Genome sequencing project",
        assets=[
            AssetRecord(asset_url="https://example.com/project.qdpx", asset_type="analysis_data"),
        ],
    )
    # Science title but has analysis_data — should be kept
    assert t.apply(record) is not None


def test_relevance_drops_empty_text() -> None:
    t = MetadataRelevanceFilter(
        name="mrf",
        keyword_keep_threshold=8.0,
        keyword_drop_threshold=-4.0,
        bypass_with_analysis_data=False,
    )
    record = _make_record(
        title=None,
        description=None,
        assets=[AssetRecord(asset_url="https://example.com/data.csv", asset_type="primary_data")],
    )
    assert t.apply(record) is None
