from __future__ import annotations

from typing import Any

from qdarchive_seeding.app.config_models import PipelineConfig, TransformSettings


def test_transforms_migrate_to_pre_transforms(minimal_config_dict: dict[str, Any]) -> None:
    minimal_config_dict = dict(minimal_config_dict)
    minimal_config_dict["transforms"] = [
        {"name": "slugify_dataset", "options": {}},
    ]

    config = PipelineConfig.model_validate(minimal_config_dict)

    assert len(config.pre_transforms) == 1
    assert isinstance(config.pre_transforms[0], TransformSettings)
    assert config.pre_transforms[0].name == "slugify_dataset"


def test_transforms_ignored_when_pre_transforms_present(
    minimal_config_dict: dict[str, Any],
) -> None:
    minimal_config_dict = dict(minimal_config_dict)
    minimal_config_dict["transforms"] = [
        {"name": "slugify_dataset", "options": {}},
    ]
    minimal_config_dict["pre_transforms"] = [
        {"name": "normalize_fields", "options": {}},
    ]

    config = PipelineConfig.model_validate(minimal_config_dict)

    assert len(config.pre_transforms) == 1
    assert config.pre_transforms[0].name == "normalize_fields"
