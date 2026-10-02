"""Keep tool progress out of assistant text presented by the public stream."""
from __future__ import annotations

import re


# The provider currently emits this exact progress sentence as response
# content. Strip it only at the public presentation boundary: inner streams
# still see activity, so a tool-only turn does not trigger a second generate.
_TOOL_PROGRESS = re.compile(
    r"(?<![\w.])[A-Za-z_][\w.-]*\([^\n]{0,500}?\) completed in \d+(?:\.\d+)?s\.\s*"
)


def assistant_text(value: str) -> str:
    return _TOOL_PROGRESS.sub("", value)
