from __future__ import annotations

import pytest

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.infra.sinks.base import BaseSink


def test_base_sink_methods_raise() -> None:
    sink = BaseSink(name="base")
    record = DatasetRecord(source_name="s", source_dataset_id="1", source_url="u")
    asset = AssetRecord(asset_url="u")

    with pytest.raises(NotImplementedError):
        sink.upsert_dataset(record)

    with pytest.raises(NotImplementedError):
        sink.upsert_asset("1", asset)

    assert sink.close() is None
