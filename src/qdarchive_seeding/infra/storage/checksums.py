from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import BinaryIO


@dataclass(slots=True)
class ChecksumComputer:
    algo: str = "sha256"

    def update_from_file(self, fh: BinaryIO, chunk_size: int) -> str:
        hasher = hashlib.new(self.algo)
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
        return hasher.hexdigest()
