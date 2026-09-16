"""Installed resources for explicitly selected optional engine modules."""
from pathlib import Path

__version__ = "1.0.0b1"


def module_assets(name: str) -> Path:
    """Return one known installed module's resource directory without mutation."""
    if name != "vault":
        raise LookupError(f"Unknown module resources: {name}")
    path = Path(__file__).resolve().parent / "resources" / name
    if not (path / "manifest.json").is_file():
        raise FileNotFoundError(f"Build module resources before packaging {name}")
    return path
