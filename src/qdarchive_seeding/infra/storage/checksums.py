from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import BinaryIO


class _NullHasher:
    """A no-op hasher that satisfies the same interface as hashlib._Hash."""

    def update(self, data: bytes) -> None:
        pass

    def hexdigest(self) -> str:
        return ""


@dataclass(slots=True)
class ChecksumComputer:
    algo: str = "sha256"

    @property
    def enabled(self) -> bool:
        return self.algo != "none"

    def create_hasher(self) -> hashlib._Hash | _NullHasher:
        """Create a new hash object for incremental updates."""
        if not self.enabled:
            return _NullHasher()
        return hashlib.new(self.algo)

    def update_from_file(self, fh: BinaryIO, chunk_size: int) -> str:
        hasher = self.create_hasher()
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
        return hasher.hexdigest()
