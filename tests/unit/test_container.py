from __future__ import annotations

import os
from pathlib import Path

from qdarchive_seeding.app.config_models import PipelineConfig
from qdarchive_seeding.app.container import _build_policy, _load_dotenv, build_container
from qdarchive_seeding.app.policies import IncrementalPolicy, RetryPolicy
from qdarchive_seeding.app.registry import create_default_registries
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
    container.sink.close()


def test_load_dotenv_parses_export_and_comments(tmp_path: Path, monkeypatch: object) -> None:
    monkeypatch.chdir(tmp_path)  # type: ignore[attr-defined]
    monkeypatch.delenv("FOO", raising=False)  # type: ignore[attr-defined]
    monkeypatch.delenv("BAR", raising=False)  # type: ignore[attr-defined]
    monkeypatch.delenv("QUOTED", raising=False)  # type: ignore[attr-defined]
    monkeypatch.delenv("UNCHANGED", raising=False)  # type: ignore[attr-defined]
    monkeypatch.setenv("UNCHANGED", "keep")  # type: ignore[attr-defined]

    (tmp_path / ".env").write_text(
        'export FOO=alpha\nBAR=bravo # comment\nQUOTED="value # not comment"\n=bad\n# ignored\n'
    )

    _load_dotenv(tmp_path / ".env")

    assert os.environ["FOO"] == "alpha"
    assert os.environ["BAR"] == "bravo"
    assert os.environ["QUOTED"] == "value # not comment"
    assert os.environ["UNCHANGED"] == "keep"


def test_build_policy_from_registry(minimal_config: PipelineConfig) -> None:
    registries = create_default_registries()

    policy = _build_policy(minimal_config, registries, force=False, retry_failed=False)
    assert isinstance(policy, IncrementalPolicy)

    policy = _build_policy(minimal_config, registries, force=False, retry_failed=True)
    assert isinstance(policy, RetryPolicy)


def test_load_dotenv_missing_file_is_noop(tmp_path: Path) -> None:
    missing = tmp_path / ".env"
    _load_dotenv(missing)
