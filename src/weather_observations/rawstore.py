from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path


class RawResponseStore:
    """Content-addressed, crash-safe store for raw source responses.

    A response is durable before its hash is returned: the bytes are written to a
    temporary file in the same directory, flushed, ``fsync``-ed, atomically
    renamed into place, and the directory entry is ``fsync``-ed.  Re-saving the
    same body is idempotent and verifies existing content instead of trusting
    the file name.
    """

    def __init__(self, directory: str | Path) -> None:
        if not str(directory):
            raise ValueError("raw response directory is required")
        self.directory = Path(directory)

    @staticmethod
    def digest(body: bytes) -> str:
        return hashlib.sha256(body).hexdigest()

    def path_for(self, digest: str) -> Path:
        return self.directory / f"{digest}.json"

    def exists(self, digest: str) -> bool:
        return self.path_for(digest).is_file()

    def verify(self, digest: str) -> bool:
        target = self.path_for(digest)
        try:
            body = target.read_bytes()
        except OSError:
            return False
        return self.digest(body) == digest

    def save(self, body: bytes) -> str:
        digest = self.digest(body)
        self.directory.mkdir(parents=True, exist_ok=True)
        target = self.path_for(digest)

        if target.exists() and self.verify(digest):
            return digest

        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", dir=self.directory, prefix=f".{digest}.", delete=False
            ) as handle:
                temporary = Path(handle.name)
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
            temporary = None
            self._fsync_directory()
        finally:
            if temporary is not None:
                try:
                    temporary.unlink()
                except FileNotFoundError:
                    pass
        if not self.verify(digest):
            raise OSError("raw response was not durably written")
        return digest

    def _fsync_directory(self) -> None:
        try:
            descriptor = os.open(self.directory, os.O_RDONLY)
        except OSError:
            # Directory fsync is not available on every platform/filesystem.
            return
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def read(self, digest: str) -> bytes:
        return self.path_for(digest).read_bytes()
