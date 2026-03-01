from __future__ import annotations

import pytest

from qdarchive_seeding.app.registry import InMemoryRegistry
from qdarchive_seeding.core.exceptions import RegistryError


def test_register_and_get() -> None:
    reg = InMemoryRegistry()
    reg.register("foo", "bar_component")
    assert reg.get("foo") == "bar_component"


def test_register_duplicate_raises() -> None:
    reg = InMemoryRegistry()
    reg.register("foo", "bar")
    with pytest.raises(RegistryError, match="already registered"):
        reg.register("foo", "baz")


def test_get_missing_raises() -> None:
    reg = InMemoryRegistry()
    with pytest.raises(RegistryError, match="not found"):
        reg.get("missing")


def test_list_returns_sorted() -> None:
    reg = InMemoryRegistry()
    reg.register("zebra", 1)
    reg.register("alpha", 2)
    assert reg.list() == ["alpha", "zebra"]


def test_spec_returns_none_by_default() -> None:
    reg = InMemoryRegistry()
    reg.register("foo", "bar")
    assert reg.spec("foo") is None
