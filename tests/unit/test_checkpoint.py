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
