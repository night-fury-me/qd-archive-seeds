from __future__ import annotations

from pathlib import Path

from qdarchive_seeding.infra.storage.paths import PathStrategy, safe_filename


def test_dataset_dir() -> None:
    strategy = PathStrategy(layout_template="{source_name}/{dataset_slug}/")
    result = strategy.dataset_dir(Path("/root"), source_name="zenodo", dataset_slug="my-dataset")
    assert result == Path("/root/zenodo/my-dataset/")


def test_asset_path() -> None:
    strategy = PathStrategy(layout_template="{source_name}/{dataset_slug}/")
    result = strategy.asset_path(
        Path("/root"), source_name="zenodo", dataset_slug="ds", filename="file.pdf"
    )
    assert result == Path("/root/zenodo/ds/file.pdf")


def test_dataset_dir_with_version() -> None:
    strategy = PathStrategy(layout_template="{source_name}/{dataset_slug}/{version}/")
    result = strategy.dataset_dir(
        Path("/root"), source_name="zenodo", dataset_slug="my-dataset", version="v1"
    )
    assert result == Path("/root/zenodo/my-dataset/v1/")


def test_dataset_dir_with_version_none_collapses() -> None:
    strategy = PathStrategy(layout_template="{source_name}/{dataset_slug}/{version}/")
    result = strategy.dataset_dir(
        Path("/root"), source_name="zenodo", dataset_slug="my-dataset", version=None
    )
    # Empty version should collapse the double slash
    assert result == Path("/root/zenodo/my-dataset/")


def test_safe_filename_normal() -> None:
    assert safe_filename("hello-world_2024.pdf") == "hello-world_2024.pdf"


def test_safe_filename_strips_special() -> None:
    assert safe_filename("file name (1).pdf") == "filename1.pdf"


def test_safe_filename_empty() -> None:
    assert safe_filename("") == "file"


def test_safe_filename_all_special() -> None:
    assert safe_filename("@#$%") == "file"
