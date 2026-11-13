"""Parallel mutating calls wait for the single-writer lease instead of failing."""
import asyncio
import tempfile
import time
from pathlib import Path

import pytest

from openagent_capability_host.lease import MutatingLease
from openagent_tool_protocol.types import HostError


def _lease(temp, **kwargs):
    return MutatingLease(Path(temp) / "state.db", **kwargs)


@pytest.mark.asyncio
async def test_second_call_waits_for_the_first_to_leave():
    with tempfile.TemporaryDirectory() as temp:
        lease = _lease(temp, wait_seconds=5)
        await lease.enter("agent", "call-1", "shell_exec")

        async def release_soon():
            await asyncio.sleep(0.3)
            await lease.leave("agent", "call-1")

        started = time.monotonic()
        await asyncio.gather(release_soon(), lease.enter("agent", "call-2", "shell_exec"))
        assert 0.2 < time.monotonic() - started < 3
        await lease.leave("agent", "call-2")


@pytest.mark.asyncio
async def test_another_principal_is_still_refused_at_once():
    with tempfile.TemporaryDirectory() as temp:
        lease = _lease(temp, wait_seconds=5)
        await lease.enter("agent", "call-1", "shell_exec")
        started = time.monotonic()
        with pytest.raises(HostError) as caught:
            await lease.enter("other", "call-2", "shell_exec")
        assert time.monotonic() - started < 1
        assert caught.value.code == "lease_held"


@pytest.mark.asyncio
async def test_wait_is_bounded():
    with tempfile.TemporaryDirectory() as temp:
        lease = _lease(temp, wait_seconds=0.5)
        await lease.enter("agent", "call-1", "shell_exec")
        started = time.monotonic()
        with pytest.raises(HostError):
            await lease.enter("agent", "call-2", "shell_exec")
        assert time.monotonic() - started < 2
