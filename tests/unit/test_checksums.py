from __future__ import annotations

import hashlib
import io

from qdarchive_seeding.infra.storage.checksums import ChecksumComputer


def test_checksum_computer_sha256_matches_hashlib() -> None:
    data = b"hello"
    computer = ChecksumComputer(algo="sha256")

    digest = computer.update_from_file(io.BytesIO(data), chunk_size=2)

    assert digest == hashlib.sha256(data).hexdigest()
    assert computer.enabled is True


def test_checksum_computer_none_returns_empty_string() -> None:
    data = b"hello"
    computer = ChecksumComputer(algo="none")

    digest = computer.update_from_file(io.BytesIO(data), chunk_size=2)

    assert digest == ""
    assert computer.enabled is False
