"""Speech adapters for the runtime."""

import asyncio
from importlib import import_module


async def _load_litellm():
    try:
        return await asyncio.to_thread(import_module, "litellm")
    except ImportError:
        return None
