from __future__ import annotations

from pathlib import Path

from qdarchive_seeding.app.config_models import PipelineConfig
from qdarchive_seeding.app.container import build_container
from qdarchive_seeding.infra.http.auth import BearerAuth


def test_build_container_loads_env_from_dotenv(
    tmp_path: Path, minimal_config: PipelineConfig, monkeypatch: object
) -> None:
    monkeypatch.chdir(tmp_path)  # type: ignore[attr-defined]
    # Remove any existing ZENODO_TOKEN so setdefault picks up our .env value
    monkeypatch.delenv("ZENODO_TOKEN", raising=False)  # type: ignore[attr-defined]
    (tmp_path / ".env").write_text("ZENODO_TOKEN=test-token\n")

    minimal_config.auth.type = "bearer"
    minimal_config.auth.env = {"token": "ZENODO_TOKEN"}

    container = build_container(minimal_config)

    assert isinstance(container.auth, BearerAuth)
    assert container.auth.token == "test-token"
