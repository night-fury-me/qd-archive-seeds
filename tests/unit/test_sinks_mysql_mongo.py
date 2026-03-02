from __future__ import annotations

from typing import Any

from qdarchive_seeding.core.entities import AssetRecord, DatasetRecord
from qdarchive_seeding.infra.sinks.mongodb import MongoDBSink
from qdarchive_seeding.infra.sinks.mysql import MySQLSink


def test_mysql_sink_executes_schema_and_upserts(monkeypatch: object) -> None:
    calls: list[dict[str, Any]] = []
    executed: list[str] = []

    class DummyCursor:
        def execute(self, sql: str, _params: Any = None) -> None:
            executed.append(sql)

        def __enter__(self) -> DummyCursor:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    class DummyConn:
        def cursor(self) -> DummyCursor:
            return DummyCursor()

        def commit(self) -> None:
            return None

        def close(self) -> None:
            return None

    def fake_connect(**kwargs: Any) -> DummyConn:
        calls.append(kwargs)
        return DummyConn()

    monkeypatch.setattr("qdarchive_seeding.infra.sinks.mysql.pymysql.connect", fake_connect)

    sink = MySQLSink(name="mysql", host="h", port=3307, database="db", user="u", password="p")
    record = DatasetRecord(source_name="s", source_dataset_id="1", source_url="u")
    dataset_id = sink.upsert_dataset(record)
    asset = AssetRecord(asset_url="https://example.com/file.pdf")
    sink.upsert_asset(dataset_id, asset)

    assert len(calls) >= 3
    assert calls[0]["host"] == "h"
    assert any("CREATE TABLE" in sql for sql in executed)


def test_mongodb_sink_creates_indexes_and_upserts(monkeypatch: object) -> None:
    index_calls: list[tuple[str, Any]] = []
    update_calls: list[dict[str, Any]] = []

    class DummyCollection:
        def __init__(self, name: str) -> None:
            self.name = name

        def create_index(self, keys: Any, unique: bool = False) -> None:
            index_calls.append((self.name, keys))

        def update_one(self, _filter: Any, _update: Any, upsert: bool = False) -> None:
            update_calls.append({"name": self.name, "upsert": upsert})

    class DummyDB:
        def __init__(self) -> None:
            self._collections = {
                "datasets": DummyCollection("datasets"),
                "assets": DummyCollection("assets"),
            }

        def __getitem__(self, name: str) -> DummyCollection:
            return self._collections[name]

    class DummyClient:
        def __init__(self, _uri: str) -> None:
            self._db = DummyDB()

        def __getitem__(self, _name: str) -> DummyDB:
            return self._db

        def close(self) -> None:
            return None

    monkeypatch.setattr("qdarchive_seeding.infra.sinks.mongodb.MongoClient", DummyClient)

    sink = MongoDBSink(name="mongodb", uri="mongodb://example", database="db")
    record = DatasetRecord(source_name="s", source_dataset_id="1", source_url="u")
    dataset_id = sink.upsert_dataset(record)
    asset = AssetRecord(asset_url="https://example.com/file.pdf")
    sink.upsert_asset(dataset_id, asset)

    assert len(index_calls) == 2
    assert update_calls[0]["name"] == "datasets"
    assert update_calls[0]["upsert"] is True
    assert update_calls[1]["name"] == "assets"
    assert update_calls[1]["upsert"] is True
