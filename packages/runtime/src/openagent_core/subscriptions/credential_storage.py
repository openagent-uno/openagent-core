"""Atomic replacement of credential files; no provider or network operations."""

import json
import os
import tempfile
from pathlib import Path


def write_credentials(path: Path, data: dict) -> None:
    # Serialize first: invalid data must not touch an existing credential file.
    payload = json.dumps(data, indent=2).encode("utf-8")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    # mkstemp creates mode 0600 on POSIX before any secret is written.
    # On Windows, access is controlled by the containing directory's ACL.
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
