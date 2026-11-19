"""Allowlisted response quota telemetry, scoped to the current protocol call."""
from contextlib import contextmanager
from contextvars import ContextVar
import math
import time

_observer = ContextVar('subscription-quota-observer', default=None)


def claude_quota(headers):
    values = {str(k).lower(): v for k, v in headers.items()}
    quota = {}
    for output, suffix in [('util_7d','7d-utilization'),('reset_7d','7d-reset'),
                           ('util_5h','5h-utilization'),('reset_5h','5h-reset')]:
        try:
            value = float(values['anthropic-ratelimit-unified-'+suffix])
            if math.isfinite(value) and value >= 0:
                quota[output] = value
        except (ValueError, TypeError, KeyError):
            pass
    status = values.get('anthropic-ratelimit-unified-7d-status')
    if status in {'allowed','allowed_warning','rejected'}:
        quota['status_7d'] = status
    claim = values.get('anthropic-ratelimit-unified-representative-claim')
    if claim in {'five_hour','seven_day','5h','7d','seven_day_sonnet','seven_day_opus','overage'}:
        quota['claim'] = claim
    if quota:
        quota['observed_at_ms'] = int(time.time()*1000)
    return quota


@contextmanager
def observe(callback):
    token = _observer.set(callback)
    try:
        yield
    finally:
        _observer.reset(token)


def report_claude(headers):
    callback = _observer.get()
    if callback is not None:
        quota = claude_quota(headers)
        if quota:
            try:
                callback(quota)
            except OSError:
                # Optional telemetry cannot turn an otherwise valid reply into
                # a provider failure. Atomic storage retains its last snapshot.
                pass
