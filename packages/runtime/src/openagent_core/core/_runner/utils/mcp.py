import json
import base64
from contextlib import contextmanager
from contextvars import ContextVar
from functools import partial
from typing import TYPE_CHECKING, Optional, Union
from urllib.parse import unquote, urlparse
from uuid import uuid4

from openagent_core.core._runner.utils.log import log_debug, log_exception

try:
    from mcp import ClientSession
    from mcp.types import (
        AudioContent,
        BlobResourceContents,
        CallToolResult,
        EmbeddedResource,
        ImageContent,
        ResourceLink,
        TextContent,
        TextResourceContents,
    )
    from mcp.types import Tool as MCPTool
except (ImportError, ModuleNotFoundError):
    raise ImportError("`mcp` not installed. Please install using `pip install mcp`")


from openagent_core.stream.media import Audio, File, Image, Video
from openagent_core.mcp._runtime.function import ToolResult
from openagent_core.memory.artifacts import safe_attachment_filename

if TYPE_CHECKING:
    from openagent_core.core._runner.agent import Agent
    from openagent_core.core._run_state import RunContext
    from openagent_core.core._runner.team.team import Team
    from openagent_core.mcp._runtime.mcp.mcp import MCPTools
    from openagent_core.mcp._runtime.mcp.multi_mcp import MultiMCPTools


# A catalog-authorized call must not migrate to a replacement connection while
# awaiting readiness, credentials, ping or provider I/O. Unscoped legacy calls
# retain their existing recovery behavior.
_capability_generation_scope: ContextVar[tuple[object, str] | None] = ContextVar(
    "openagent_mcp_capability_generation", default=None
)


def _check_capability_generation(instance) -> None:
    expected = _capability_generation_scope.get()
    if expected is not None and expected[0] is instance:
        if getattr(instance, "capability_generation", None) != expected[1]:
            from openagent_core.capabilities import CapabilityUnavailable
            raise CapabilityUnavailable("The exact MCP connection changed or its catalog is unavailable")


@contextmanager
def mcp_capability_generation_scope(instance):
    generation = getattr(instance, "capability_generation", NotImplemented)
    if generation is NotImplemented:
        yield lambda: None
        return
    if generation is None:
        from openagent_core.capabilities import CapabilityUnavailable
        raise CapabilityUnavailable("The current MCP connection has no complete catalog")
    token = _capability_generation_scope.set((instance, generation))
    try:
        yield lambda: _check_capability_generation(instance)
        _check_capability_generation(instance)
    finally:
        _capability_generation_scope.reset(token)


# MCP payloads are already resident when the SDK hands them to us, but these
# limits prevent a tool from multiplying memory during base64 decode or placing
# an unbounded embedded resource in the model context. Output media has the
# same 50 MiB ceiling at the NativeProvider/CAS boundary.
_MCP_MEDIA_MAX_BYTES = 50 * 1024 * 1024
_MCP_MEDIA_MAX_TOTAL_BYTES = 100 * 1024 * 1024
_MCP_TEXT_MAX_CHARS = 100_000
_MCP_TEXT_MAX_BYTES = 256 * 1024
_MCP_COMBINED_TEXT_MAX_CHARS = 200_000
_MCP_COMBINED_TEXT_MAX_BYTES = 512 * 1024
_MCP_CONTENT_MAX_ITEMS = 32
_MCP_URI_MAX_CHARS = 2_048


def _decode_mcp_base64(
    value: object,
    *,
    label: str,
    remaining_bytes: int | None = None,
) -> bytes:
    """Strictly decode one bounded MCP base64 field."""
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} has no base64 data")
    # Check before decode so an oversized text field cannot allocate another
    # comparably large bytes object. The final decoded length remains the
    # authoritative guard because padding can vary.
    limit = _MCP_MEDIA_MAX_BYTES
    if remaining_bytes is not None:
        limit = min(limit, max(0, int(remaining_bytes)))
    max_encoded = 4 * ((limit + 2) // 3)
    try:
        encoded = value.encode("ascii")
    except UnicodeEncodeError as exc:
        raise ValueError(f"{label} contains non-ASCII base64 data") from exc
    if len(encoded) > max_encoded:
        raise ValueError(
            f"{label} exceeds the {limit}-byte remaining media limit"
        )
    try:
        decoded = base64.b64decode(encoded, validate=True)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"{label} contains malformed base64 data") from exc
    if len(decoded) > limit:
        raise ValueError(
            f"{label} exceeds the {limit}-byte remaining media limit"
        )
    return decoded


def _bounded_mcp_text(
    value: object,
    *,
    label: str,
    max_chars: int | None = None,
    max_bytes: int | None = None,
) -> tuple[str, bool]:
    """Return bounded UTF-8 text plus whether truncation was necessary."""
    char_limit = _MCP_TEXT_MAX_CHARS if max_chars is None else max(0, int(max_chars))
    byte_limit = _MCP_TEXT_MAX_BYTES if max_bytes is None else max(0, int(max_bytes))
    text = str(value or "")
    truncated = len(text) > char_limit
    if truncated:
        text = text[:char_limit]
    encoded = text.encode("utf-8")
    if len(encoded) > byte_limit:
        encoded = encoded[:byte_limit]
        text = encoded.decode("utf-8", errors="ignore")
        truncated = True
    if truncated:
        text += f"\n[{label} truncated by OpenAgent]"
    return text, truncated


_SESSION_DEAD_ERROR_NAMES = frozenset({
    "ClosedResourceError",
    "BrokenResourceError",
    "EndOfStream",
})


def _session_is_dead(exc: BaseException) -> bool:
    """Whether ``exc`` means the MCP session's streams are gone for good.

    A streamable-HTTP or stdio session whose memory streams closed (one timed
    out POST tears down the transport task group) raises one of these on every
    later send; nothing but a new session recovers it. Walks causes, contexts
    and exception groups because anyio/the MCP SDK wrap them.
    """
    seen: set[int] = set()
    stack: list[BaseException] = [exc]
    while stack:
        item = stack.pop()
        if item is None or id(item) in seen:
            continue
        seen.add(id(item))
        if type(item).__name__ in _SESSION_DEAD_ERROR_NAMES:
            return True
        if type(item).__name__ == "McpError" and "connection closed" in str(item).lower():
            return True
        stack.extend(getattr(item, "exceptions", ()) or ())
        stack.append(item.__cause__)
        stack.append(item.__context__)
    return False


def _describe_error(exc: BaseException) -> str:
    # ``str(ClosedResourceError())`` is empty: "Error: " told nobody anything.
    text = str(exc)
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__


def _failed_tool_result(tool_name: str, exc: BaseException) -> ToolResult:
    """A failed call, marked as failed in the MCP envelope callers inspect.

    ``content`` keeps the historical ``Error: ...`` string for the model. The
    envelope carries ``isError`` so tool-search, workflows and product callers
    can never read a transport failure as a completed action.
    """
    detail = _describe_error(exc)
    text = f"Error: {detail}"
    return ToolResult(
        content=text,
        mcp_result={
            "content": [{"type": "text", "text": text, "annotations": None, "_meta": None}],
            "structuredContent": None,
            "isError": True,
            "_meta": {
                "openagent/transport_error": {
                    "tool": tool_name,
                    "type": type(exc).__name__,
                    "session_dead": _session_is_dead(exc),
                },
            },
        },
    )


async def _reconnect_session(mcp_tools_instance, dead_session, exc: BaseException) -> bool:
    reconnect = getattr(mcp_tools_instance, "_openagent_reconnect", None) if mcp_tools_instance else None
    if not callable(reconnect):
        return False
    try:
        return bool(await reconnect(dead_session, reason=type(exc).__name__))
    except Exception as e:  # noqa: BLE001 — never mask the original failure
        log_exception(e)
        return False


def get_entrypoint_for_tool(
    tool: MCPTool,
    session: ClientSession,
    mcp_tools_instance: Optional[Union["MCPTools", "MultiMCPTools"]] = None,
    server_idx: int = 0,
):
    """
    Return an entrypoint for an MCP tool.

    Args:
        tool: The MCP tool to create an entrypoint for
        session: The MCP ClientSession to use
        mcp_tools_instance: Optional MCPTools or MultiMCPTools instance
        server_idx: Index of the server (for MultiMCPTools)

    Returns:
        Callable: The entrypoint function for the tool
    """

    async def call_tool(
        tool_name: str,
        run_context: Optional["RunContext"] = None,
        agent: Optional["Agent"] = None,
        team: Optional["Team"] = None,
        **kwargs,
    ) -> ToolResult:
        async def _resolve_session():
            # Get the appropriate session for this run
            # If mcp_tools_instance has header_provider and run_context is provided,
            # this will create/reuse a session with dynamic headers
            if mcp_tools_instance and hasattr(mcp_tools_instance, "get_session_for_run"):
                # Import here to avoid circular imports
                from openagent_core.mcp._runtime.mcp.multi_mcp import MultiMCPTools

                # For MultiMCPTools, pass server_idx; for MCPTools, only pass run_context
                if isinstance(mcp_tools_instance, MultiMCPTools):
                    return await mcp_tools_instance.get_session_for_run(
                        run_context=run_context, server_idx=server_idx, agent=agent, team=team
                    )
                return await mcp_tools_instance.get_session_for_run(
                    run_context=run_context, agent=agent, team=team
                )
            return session

        active_session = None
        # Execute the MCP tool call
        try:
            # A reconnect in flight publishes its session before the handshake
            # completes: wait for it rather than calling on a half-open session.
            await_ready = getattr(mcp_tools_instance, "_openagent_await_ready", None) if mcp_tools_instance else None
            if callable(await_ready) and not await await_ready():
                raise ConnectionError(
                    "MCP session reconnect did not complete; the request was not sent"
                )
            _check_capability_generation(mcp_tools_instance)
            try:
                active_session = await _resolve_session()
            except ValueError as e:
                # "Session is not initialized": an earlier reconnect failed and
                # left the toolkit without a session. Try again rather than
                # failing every later call until the process restarts.
                if not await _reconnect_session(mcp_tools_instance, None, e):
                    raise
                active_session = await _resolve_session()

            _check_capability_generation(mcp_tools_instance)
            try:
                await active_session.send_ping()
            except Exception as e:
                if not _session_is_dead(e):
                    log_exception(e)
                else:
                    # The request below has not been sent, so replacing the
                    # session and calling once on the new one cannot repeat a
                    # side effect. Without a live session, fail the call
                    # visibly instead of sending into a closed stream.
                    log_exception(
                        f"MCP session for tool '{tool_name}' is dead "
                        f"({_describe_error(e)}); reconnecting"
                    )
                    if not await _reconnect_session(mcp_tools_instance, active_session, e):
                        log_exception(f"Failed to call MCP tool '{tool_name}': {_describe_error(e)}")
                        return _failed_tool_result(tool_name, e)
                    active_session = await _resolve_session()

            # Stamp dry-run meta when the current run is a dry-run so the MCP
            # server captures/rejects writes instead of executing them. None on
            # a live run (identical to not passing meta at all).
            from openagent_core.core.dry_run import call_meta

            log_debug(f"Calling MCP Tool '{tool_name}' with args: {kwargs}")
            _check_capability_generation(mcp_tools_instance)
            result: CallToolResult = await active_session.call_tool(
                tool_name, kwargs, meta=call_meta()
            )  # type: ignore

            _check_capability_generation(mcp_tools_instance)

            # Preserve the COMPLETE MCP envelope.  The runtime's historical
            # path below turns content blocks into a human/model-facing string
            # and media objects, but that representation cannot carry
            # structuredContent, isError, _meta, ResourceLink, AudioContent or
            # future protocol block types.  ``mode='json'`` also makes AnyUrl
            # and other pydantic values safe for the next tool-search boundary.
            try:
                mcp_result = result.model_dump(
                    mode="json", by_alias=True, exclude_none=False,
                )
            except Exception:
                mcp_result = json.loads(result.model_dump_json(by_alias=True))

            # Process the result content
            response_str = ""
            images = []
            audios = []
            videos = []
            files = []
            media_bytes_total = 0

            def decode_media(value: object, *, label: str) -> bytes:
                nonlocal media_bytes_total
                decoded = _decode_mcp_base64(
                    value,
                    label=label,
                    remaining_bytes=_MCP_MEDIA_MAX_TOTAL_BYTES - media_bytes_total,
                )
                media_bytes_total += len(decoded)
                return decoded

            content_items = result.content[:_MCP_CONTENT_MAX_ITEMS]
            for content_item in content_items:
                if isinstance(content_item, TextContent):
                    text_content, text_truncated = _bounded_mcp_text(
                        content_item.text, label="MCP text content",
                    )

                    # Parse as JSON to check for custom image format
                    try:
                        # A truncated payload is no longer valid JSON and must
                        # remain visible as bounded text, not be reparsed.
                        parsed_json = None if text_truncated else json.loads(text_content)
                        if (
                            isinstance(parsed_json, dict)
                            and parsed_json.get("type") == "image"
                            and "data" in parsed_json
                        ):
                            log_debug("Found custom JSON image format in TextContent")

                            # Extract image data
                            image_data = parsed_json.get("data")
                            mime_type = parsed_json.get("mimeType", "image/png")

                            if image_data and isinstance(image_data, str):
                                try:
                                    image_bytes = decode_media(
                                        image_data, label="MCP JSON image",
                                    )
                                except ValueError as e:
                                    log_debug(str(e))
                                    response_str += f"[Rejected MCP JSON image: {e}]\n"
                                    image_bytes = None

                                if image_bytes:
                                    img_artifact = Image(
                                        id=str(uuid4()),
                                        url=None,
                                        content=image_bytes,
                                        mime_type=mime_type,
                                    )
                                    images.append(img_artifact)
                                    response_str += "Image has been generated and added to the response.\n"
                                    continue

                    except (json.JSONDecodeError, TypeError):
                        pass

                    response_str += text_content + "\n"

                elif isinstance(content_item, ImageContent):
                    # Handle standard MCP ImageContent
                    image_data = getattr(content_item, "data", None)

                    try:
                        image_data = decode_media(
                            image_data, label="MCP image",
                        )
                    except ValueError as e:
                        log_debug(str(e))
                        response_str += f"[Rejected MCP image: {e}]\n"
                        continue

                    img_artifact = Image(
                        id=str(uuid4()),
                        content=image_data,
                        mime_type=getattr(content_item, "mimeType", "image/png"),
                    )
                    images.append(img_artifact)
                    response_str += "Image has been generated and added to the response.\n"
                elif isinstance(content_item, AudioContent):
                    audio_data = getattr(content_item, "data", None)
                    try:
                        audio_data = decode_media(
                            audio_data, label="MCP audio",
                        )
                    except ValueError as e:
                        log_debug(str(e))
                        response_str += f"[Rejected MCP audio: {e}]\n"
                        continue
                    mime = getattr(content_item, "mimeType", None) or "audio/mpeg"
                    fmt = mime.split("/", 1)[-1].split(";", 1)[0]
                    audios.append(Audio(
                        id=str(uuid4()),
                        content=audio_data,
                        mime_type=mime,
                        format=fmt,
                    ))
                    response_str += "Audio has been generated and added to the response.\n"
                elif isinstance(content_item, EmbeddedResource):
                    resource = content_item.resource
                    uri = str(getattr(resource, "uri", "") or "")
                    uri_display = (
                        uri
                        if len(uri) <= _MCP_URI_MAX_CHARS
                        else uri[:_MCP_URI_MAX_CHARS] + "…"
                    )
                    parsed_uri = urlparse(uri)
                    filename = safe_attachment_filename(
                        unquote(parsed_uri.path), fallback="resource",
                    )
                    mime = getattr(resource, "mimeType", None)
                    if isinstance(resource, TextResourceContents):
                        resource_text, _ = _bounded_mcp_text(
                            resource.text, label="MCP embedded text resource",
                        )
                        response_str += (
                            f"[Embedded resource {uri_display or filename}]\n"
                            f"{resource_text}\n"
                        )
                    elif isinstance(resource, BlobResourceContents):
                        try:
                            blob = decode_media(
                                resource.blob, label="MCP embedded resource",
                            )
                        except ValueError as e:
                            log_debug(str(e))
                            response_str += f"[Rejected MCP embedded resource: {e}]\n"
                            blob = None
                        if blob is not None:
                            media_id = str(uuid4())
                            mime = str(mime or "").split(";", 1)[0].lower()
                            fmt = (
                                filename.rsplit(".", 1)[-1].lower()
                                if "." in filename else None
                            )
                            if mime.startswith("image/"):
                                images.append(Image(
                                    id=media_id,
                                    content=blob,
                                    mime_type=mime,
                                    format=fmt,
                                ))
                            elif mime.startswith("audio/"):
                                audios.append(Audio(
                                    id=media_id,
                                    content=blob,
                                    mime_type=mime,
                                    format=fmt,
                                ))
                            elif mime.startswith("video/"):
                                videos.append(Video(
                                    id=media_id,
                                    content=blob,
                                    mime_type=mime,
                                    format=fmt,
                                ))
                            else:
                                file_kwargs = {
                                    "id": media_id,
                                    "content": blob,
                                    "filename": filename,
                                    "format": fmt,
                                }
                                if mime in File.valid_mime_types():
                                    file_kwargs["mime_type"] = mime
                                files.append(File(**file_kwargs))
                            response_str += (
                                f"Embedded resource {uri_display or filename} has been added "
                                "to the response.\n"
                            )
                elif isinstance(content_item, ResourceLink):
                    uri = str(getattr(content_item, "uri", "") or "")
                    uri_display = (
                        uri
                        if len(uri) <= _MCP_URI_MAX_CHARS
                        else uri[:_MCP_URI_MAX_CHARS] + "…"
                    )
                    link_name, _ = _bounded_mcp_text(
                        getattr(content_item, "name", None) or "resource",
                        label="MCP resource link name",
                        max_chars=512,
                        max_bytes=2_048,
                    )
                    response_str += f"[Resource link {link_name}: {uri_display}]\n"
                else:
                    # Future MCP block types remain intact in ``mcp_result``.
                    response_str += (
                        "[Unsupported content type: "
                        f"{getattr(content_item, 'type', type(content_item).__name__)}]\n"
                    )

            if len(result.content) > len(content_items):
                response_str += (
                    f"[Omitted {len(result.content) - len(content_items)} additional "
                    "MCP content items: payload limit reached.]\n"
                )

            response_str, _ = _bounded_mcp_text(
                response_str,
                label="MCP combined tool output",
                max_chars=_MCP_COMBINED_TEXT_MAX_CHARS,
                max_bytes=_MCP_COMBINED_TEXT_MAX_BYTES,
            )

            display_content = response_str.strip()
            if result.isError:
                detail = display_content or "MCP tool returned no displayable error content"
                display_content = f"Error from MCP tool '{tool_name}': {detail}"
                display_content, _ = _bounded_mcp_text(
                    display_content,
                    label="MCP combined error output",
                    max_chars=_MCP_COMBINED_TEXT_MAX_CHARS,
                    max_bytes=_MCP_COMBINED_TEXT_MAX_BYTES,
                )
            return ToolResult(
                content=display_content,
                images=images if images else None,
                audios=audios if audios else None,
                videos=videos if videos else None,
                files=files if files else None,
                mcp_result=mcp_result,
            )
        except Exception as e:
            log_exception(f"Failed to call MCP tool '{tool_name}': {_describe_error(e)}")
            if _session_is_dead(e) and active_session is not None:
                # The request may already have been written, so it is never
                # retried here; only the NEXT call gets a fresh session.
                await _reconnect_session(mcp_tools_instance, active_session, e)
            return _failed_tool_result(tool_name, e)

    return partial(call_tool, tool_name=tool.name)


def prepare_command(command: str) -> list[str]:
    """Sanitize a command and split it into parts before using it to run a MCP server."""
    import os
    import shutil
    from shlex import split

    # Block dangerous characters
    if any(char in command for char in ["&", "|", ";", "`", "$", "(", ")"]):
        raise ValueError("MCP command can't contain shell metacharacters")

    parts = split(command)
    if not parts:
        raise ValueError("MCP command can't be empty")

    # Only allow specific executables
    ALLOWED_COMMANDS = {
        # Python
        "python",
        "python3",
        "uv",
        "uvx",
        "pipx",
        # Node
        "node",
        "npm",
        "npx",
        "yarn",
        "pnpm",
        "bun",
        # Other runtimes
        "deno",
        "java",
        "ruby",
        "docker",
    }

    executable = parts[0].split("/")[-1]

    # Check if it's a relative path starting with ./ or ../
    if executable.startswith("./") or executable.startswith("../"):
        # Allow relative paths to binaries
        return parts

    # Check if it's an absolute path to a binary
    if executable.startswith("/") and os.path.isfile(executable):
        # Allow absolute paths to existing files
        return parts

    # Check if it's a binary in current directory without ./
    if "/" not in executable and os.path.isfile(executable):
        # Allow binaries in current directory
        return parts

    # Check if it's a binary in PATH
    if shutil.which(executable):
        return parts

    if executable not in ALLOWED_COMMANDS:
        raise ValueError(f"MCP command needs to use one of the following executables: {ALLOWED_COMMANDS}")

    first_part = parts[0]
    executable = first_part.split("/")[-1]

    # Allow known commands
    if executable in ALLOWED_COMMANDS:
        return parts

    # Allow relative paths to custom binaries
    if first_part.startswith(("./", "../")):
        return parts

    # Allow absolute paths to existing files
    if first_part.startswith("/") and os.path.isfile(first_part):
        return parts

    # Allow binaries in current directory without ./
    if "/" not in first_part and os.path.isfile(first_part):
        return parts

    # Allow binaries in PATH
    if shutil.which(first_part):
        return parts

    raise ValueError(f"MCP command needs to use one of the following executables: {ALLOWED_COMMANDS}")
