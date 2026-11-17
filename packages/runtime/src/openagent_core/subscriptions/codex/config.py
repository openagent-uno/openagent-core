"""Explicit instance-owned subscription settings. No environment or process state is read."""
from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

def _parse_models() -> List[str]:
    return ['gpt-5.6-sol', 'gpt-5.6-sol:low', 'gpt-5.6-sol:high', 'gpt-5.6-sol:xhigh', 'gpt-5.6-luna', 'gpt-5.3-codex-spark']

def _parse_model_map() -> Dict[str, str]:
    return {}

@dataclass
class Settings:
    creds_file: Path
    transport: Optional[str] = None
    impersonate: str = 'chrome'
    request_timeout: float = 1800.0
    default_reasoning_effort: str = 'medium'
    reasoning_summary: str = 'auto'
    emit_reasoning_content: bool = True
    reasoning_replay: bool = True
    reasoning_cache_size: int = 512
    api_key: Optional[str] = None
    admin_api_key: Optional[str] = None
    upstream_max_concurrency: int = 2
    upstream_max_queue: int = 32
    upstream_admission_timeout: float = 5.0
    upstream_queue_timeout: float = 900.0
    upstream_min_interval: float = 1.0
    rate_limit_retries: int = 2
    rate_limit_backoff: float = 15.0
    rate_limit_max_backoff: float = 120.0
    account_rate_limit_cooldown: float = 360.0
    account_rate_limit_max_cooldown: float = 10800.0
    account_rate_limit_probe_seconds: float = 120.0
    image_model: str = 'gpt-5.6-sol'
    image_enabled: bool = True
    usage_enabled: bool = True
    usage_file: Optional[str] = None
    usage_retain_days: int = 30
    realtime_enabled: bool = True
    realtime_model: str = 'gpt-realtime-1.5'
    realtime_hold_seconds: float = 1200.0
    realtime_voice: str = ''
    realtime_instructions: str = ''
    models: List[str] = field(default_factory=_parse_models)
    model_map: Dict[str, str] = field(default_factory=_parse_model_map)
