"""Content-addressed, write-once store for raw files (docs/PROJECT_BRIEF.md §10).

A file's name IS the SHA-256 of its bytes: data/raw/sha256/ab/abcdef....csv.gz.
Consequences worth explaining in an interview:

* Two versions of the same archive key (a revision) have different hashes, so both are
  kept. Nothing is ever overwritten with different content.
* Writing is atomic: bytes go to a temporary file in the same directory, then are
  renamed into place. A crash leaves either no file or a complete one, never half.
* If two writers race, both write identical bytes (same hash), so the rename cannot
  change content. That is why a plain atomic rename is enough here.
* Reads re-hash the bytes: silent corruption on disk is detected, not trusted.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path


class CorruptObjectError(RuntimeError):
    """Stored bytes no longer match the hash they are filed under."""


def sha256_hex(data: bytes) -> str:
    """Hex SHA-256 of `data`."""
    return hashlib.sha256(data).hexdigest()


class RawStore:
    """Write-once store rooted at `root` (usually data/raw)."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def path_for(self, digest: str) -> Path:
        """Where the object with this SHA-256 lives."""
        if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError(f"not a sha256 hex digest: {digest!r}")
        return self.root / "sha256" / digest[:2] / f"{digest}.csv.gz"

    def put(self, data: bytes) -> tuple[str, bool]:
        """Store `data`. Returns (sha256, created). Re-putting the same bytes is a no-op."""
        digest = sha256_hex(data)
        path = self.path_for(digest)
        if path.exists():
            self.get(digest)  # verifies the existing copy; raises if corrupt
            return digest, False
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp-", suffix=".part")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise
        path.chmod(0o444)  # best effort: read-only on POSIX
        return digest, True

    def get(self, digest: str) -> bytes:
        """Bytes of an object, verified against its name."""
        data = self.path_for(digest).read_bytes()
        if sha256_hex(data) != digest:
            raise CorruptObjectError(f"content of {digest} does not match its hash")
        return data

    def has(self, digest: str) -> bool:
        """True if an object with this hash is stored."""
        return self.path_for(digest).exists()


def write_atomic(path: Path, data: bytes) -> None:
    """Replace `path` with `data` atomically (used for the landing mirror)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp-", suffix=".part")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
