from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from qdarchive_seeding.core.exceptions import RegistryError
from qdarchive_seeding.core.interfaces import ComponentSpec, Registry


@dataclass(slots=True)
class RegistryEntry:
    component: Any
    spec: ComponentSpec | None = None


class InMemoryRegistry(Registry):
    def __init__(self) -> None:
        self._items: dict[str, RegistryEntry] = {}

    def register(self, name: str, component: Any, spec: ComponentSpec | None = None) -> None:
        if name in self._items:
            raise RegistryError(f"Component already registered: {name}")
        self._items[name] = RegistryEntry(component=component, spec=spec)

    def get(self, name: str) -> Any:
        try:
            return self._items[name].component
        except KeyError as exc:
            raise RegistryError(f"Component not found: {name}") from exc

    def spec(self, name: str) -> ComponentSpec | None:
        try:
            return self._items[name].spec
        except KeyError as exc:
            raise RegistryError(f"Component not found: {name}") from exc

    def list(self) -> list[str]:
        return sorted(self._items.keys())
