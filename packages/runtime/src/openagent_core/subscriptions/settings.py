"""Validated nonsecret protocol options for host-owned subscription connections."""
from __future__ import annotations

from dataclasses import fields
import importlib
import json
import math
import re
from pathlib import Path

_OPTIONS = frozenset({'transport', 'impersonate', 'default_max_tokens', 'request_timeout',
    'chunk_timeout', 'max_tools', 'upstream_max_concurrency', 'upstream_max_queue',
    'upstream_admission_timeout', 'upstream_queue_timeout', 'upstream_min_interval',
    'rate_limit_retries', 'rate_limit_backoff', 'rate_limit_max_backoff',
    'account_rate_limit_cooldown', 'models', 'model_map', 'default_reasoning_effort',
    'reasoning_summary'})
_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}')
_INTS = {'default_max_tokens', 'max_tools', 'upstream_max_concurrency',
         'upstream_max_queue', 'rate_limit_retries'}


def store_layout(value='flat') -> str:
    """A fixed layout choice, never a caller-supplied filesystem path."""
    if not isinstance(value, str) or value not in {'flat', 'provider'}:
        raise ValueError('subscription_store_layout must be flat or provider')
    return value


def store_directory(db_path: Path, provider: str, layout='flat') -> Path:
    if provider not in {'claude', 'codex'}:
        raise ValueError('Unknown subscription provider')
    root = db_path.parent / 'subscriptions'
    return root / provider if store_layout(layout) == 'provider' else root


def validate_settings(provider: str, options: dict | None) -> dict:
    if provider not in {'claude', 'codex'}:
        raise ValueError('Unknown subscription provider')
    if options is None:
        return {}
    if not isinstance(options, dict) or len(json.dumps(options)) > 32768:
        raise ValueError('Subscription settings must be a bounded object')
    config = importlib.import_module(f'openagent_core.subscriptions.{provider}.config')
    allowed = _OPTIONS & {f.name for f in fields(config.Settings)}
    if set(options) - allowed:
        raise ValueError('Unsupported or private subscription setting')
    for key, value in options.items():
        if key == 'transport':
            if value not in {None, 'httpx', 'curl_cffi'}:
                raise ValueError('Unknown subscription transport')
        elif key in {'impersonate', 'default_reasoning_effort', 'reasoning_summary'}:
            if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9._-]{1,64}', value):
                raise ValueError('Invalid subscription setting: ' + key)
        elif key == 'models':
            if (not isinstance(value, list) or len(value) > 128
                    or not all(isinstance(v, str) and _ID.fullmatch(v) for v in value)):
                raise ValueError('Invalid subscription model list')
        elif key == 'model_map':
            if (not isinstance(value, dict) or len(value) > 128
                    or not all(isinstance(k, str) and isinstance(v, str) and _ID.fullmatch(k)
                               and _ID.fullmatch(v) for k, v in value.items())):
                raise ValueError('Invalid subscription model map')
        elif value is None and key == 'chunk_timeout':
            pass
        else:
            if (type(value) not in ({int} if key in _INTS else {int, float})
                    or not math.isfinite(value) or value < 0 or value > (1_048_576 if key == 'default_max_tokens' else 86400)
                    or value == 0 and key not in {'upstream_min_interval', 'upstream_max_queue', 'rate_limit_retries'}):
                raise ValueError('Invalid subscription setting: ' + key)
    return json.loads(json.dumps(options))


def native_settings(provider: str, store: Path, options=None, *, admission_path=None):
    config = importlib.import_module(f'openagent_core.subscriptions.{provider}.config')
    return config.Settings(creds_file=store, admission_path=admission_path,
                           **validate_settings(provider, options))
