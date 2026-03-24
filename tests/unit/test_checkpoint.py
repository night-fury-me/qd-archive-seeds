from __future__ import annotations

from pathlib import Path

from qdarchive_seeding.app.checkpoint import CheckpointManager


class TestCheckpointManager:
    def test_save_and_load(self, tmp_path: Path) -> None:
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test")
        cp.mark_page("query_a", 1, 100)
        cp.mark_page("query_a", 2, 50)
        cp.mark_query_complete("query_a")

        # Load fresh from file
        cp2 = CheckpointManager(_path=tmp_path, _pipeline_id="test")
        assert cp2.is_query_complete("query_a")
        assert cp2.get_start_page("query_a") == 2

    def test_is_query_complete_false_by_default(self, tmp_path: Path) -> None:
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test")
        assert not cp.is_query_complete("unknown_query")

    def test_get_start_page_zero_for_new_query(self, tmp_path: Path) -> None:
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test")
        assert cp.get_start_page("new_query") == 0

    def test_get_start_page_returns_last_page(self, tmp_path: Path) -> None:
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test")
        cp.mark_page("query_x", 5, 100)
        assert cp.get_start_page("query_x") == 5

    def test_records_yielded_accumulates(self, tmp_path: Path) -> None:
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test")
        cp.mark_page("q", 1, 100)
        cp.mark_page("q", 2, 80)
        cp2 = CheckpointManager(_path=tmp_path, _pipeline_id="test")
        assert cp2._queries["q"].records_yielded == 180

    def test_clear_deletes_file(self, tmp_path: Path) -> None:
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test")
        cp.mark_page("q", 1, 10)
        assert cp.file_path.exists()
        cp.clear()
        assert not cp.file_path.exists()

    def test_clear_on_empty_is_safe(self, tmp_path: Path) -> None:
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test")
        cp.clear()  # Should not raise

    def test_corrupt_file_starts_fresh(self, tmp_path: Path) -> None:
        fp = tmp_path / "test_checkpoint.json"
        fp.write_text("not valid json{{{")
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test")
        assert len(cp._queries) == 0

    def test_separate_pipelines_have_separate_checkpoints(self, tmp_path: Path) -> None:
        cp1 = CheckpointManager(_path=tmp_path, _pipeline_id="pipe_a")
        cp2 = CheckpointManager(_path=tmp_path, _pipeline_id="pipe_b")
        cp1.mark_page("q", 1, 10)
        assert cp2.get_start_page("q") == 0  # Different pipeline

    # --- Failed page tracking ---

    def test_mark_page_failed_records_page(self, tmp_path: Path) -> None:
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test")
        cp.mark_page_failed("q", 3)
        assert cp.get_failed_pages("q") == [3]

    def test_mark_page_failed_no_duplicates(self, tmp_path: Path) -> None:
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test")
        cp.mark_page_failed("q", 3)
        cp.mark_page_failed("q", 3)
        assert cp.get_failed_pages("q") == [3]

    def test_clear_failed_page(self, tmp_path: Path) -> None:
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test")
        cp.mark_page_failed("q", 3)
        cp.mark_page_failed("q", 7)
        cp.clear_failed_page("q", 3)
        assert cp.get_failed_pages("q") == [7]

    def test_get_failed_pages_empty_for_unknown(self, tmp_path: Path) -> None:
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test")
        assert cp.get_failed_pages("unknown") == []

    def test_has_unresolved_failures(self, tmp_path: Path) -> None:
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test")
        assert not cp.has_unresolved_failures()
        cp.mark_page_failed("q", 5)
        assert cp.has_unresolved_failures()
        cp.clear_failed_page("q", 5)
        assert not cp.has_unresolved_failures()

    def test_failed_pages_persisted_across_load(self, tmp_path: Path) -> None:
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test")
        cp.mark_page_failed("q", 2)
        cp.mark_page_failed("q", 8)
        cp2 = CheckpointManager(_path=tmp_path, _pipeline_id="test")
        assert cp2.get_failed_pages("q") == [2, 8]
        assert cp2.has_unresolved_failures()

    # --- Date slice caching ---

    def test_date_slices_cache(self, tmp_path: Path) -> None:
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test")
        assert cp.get_date_slices("q") is None
        slices = [("2020-01-01", "2021-06-15"), ("2021-06-16", "2023-12-31")]
        cp.set_date_slices("q", slices)
        assert cp.get_date_slices("q") == slices

    def test_date_slices_persisted_across_load(self, tmp_path: Path) -> None:
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test")
        slices = [("2013-01-01", "2018-06-15"), ("2018-06-16", "2026-03-24")]
        cp.set_date_slices("ext batch 1", slices)
        cp2 = CheckpointManager(_path=tmp_path, _pipeline_id="test")
        assert cp2.get_date_slices("ext batch 1") == slices

    def test_clear_removes_date_slices(self, tmp_path: Path) -> None:
        cp = CheckpointManager(_path=tmp_path, _pipeline_id="test")
        cp.set_date_slices("q", [("2020-01-01", "2021-01-01")])
        cp.clear()
        assert cp.get_date_slices("q") is None
