"""Explicit instance-owned subscription settings. No environment or process state is read."""
from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

def _parse_models() -> List[str]:
    return ['claude-opus-5-5', 'claude-sonnet-5-5', 'claude-haiku-5-5']

def _parse_model_map() -> Dict[str, str]:
    return {}

@dataclass
class Settings:
    creds_file: Path
    admission_path: Optional[Path] = None
    require_completed: bool = False
    transport: Optional[str] = None
    impersonate: str = 'chrome'
    default_max_tokens: int = 16384
    request_timeout: float = 1800.0
    chunk_timeout: Optional[float] = 30.0
    max_tools: int = 19
    api_key: Optional[str] = None
    admin_api_key: Optional[str] = None
    upstream_max_concurrency: int = 1
    upstream_max_queue: int = 32
    upstream_admission_timeout: float = 5.0
    upstream_queue_timeout: float = 900.0
    upstream_min_interval: float = 4.0
    rate_limit_retries: int = 2
    rate_limit_backoff: float = 20.0
    rate_limit_max_backoff: float = 180.0
    account_rate_limit_cooldown: float = 360.0
    models: List[str] = field(default_factory=_parse_models)
    model_map: Dict[str, str] = field(default_factory=_parse_model_map)
