"""Read-only compatibility helpers for resources in packaged hosts.

Installer mutation, SSL preparation and updater behavior belong to the product.
"""
import sys
from pathlib import Path

def is_frozen() -> bool:
    return bool(getattr(sys,'frozen',False))

def bundle_dir() -> Path:
    return Path(getattr(sys,'_MEIPASS',Path(sys.executable).resolve().parent))

def executable_path() -> Path:
    return Path(sys.executable).resolve()
