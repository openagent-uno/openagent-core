"""Bounded visual previews for host-admitted inline MP4 attachments."""

from __future__ import annotations

import asyncio
import json
import math
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory


MAX_VIDEO_PREVIEWS_PER_TURN = 2
MAX_VIDEO_PREVIEW_BYTES = 8 * 1024 * 1024
_MAX_DURATION = 120
_MAX_PIXELS = 4_194_304
_MAX_ALLOCATION = "33554432"
_MAX_FRAME_BYTES = 192 * 1024
_FRAME_EDGE = 384
_SAMPLE_POSITIONS = (0.0, 0.45, 0.85)
_MAX_SAMPLE_COUNT = 12
_LOCAL_MP4 = (
    "-protocol_whitelist", "file", "-f", "mov",
    "-enable_drefs", "0", "-use_absolute_path", "0",
)


@dataclass(frozen=True)
class VideoFrame:
    """A JPEG and its approximate position in the original video."""

    content: bytes
    timestamp_seconds: float


async def _bounded_output(
    executable: str, args: tuple[str, ...], limit: int,
) -> bytes:
    process = await asyncio.create_subprocess_exec(
        executable, *args, stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        assert process.stdout is not None
        try:
            await process.stdout.readexactly(limit + 1)
        except asyncio.IncompleteReadError as exc:
            return exc.partial if await process.wait() == 0 else b""
        return b""
    finally:
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            # Drain the bounded pipe buffer so the killed child can be reaped.
            await process.communicate()


class FfmpegVideoFrameExtractor:
    """Optional visual fallback using trusted local FFmpeg and FFprobe binaries.

    Only inline MP4 bytes are accepted. The extractor opens its own temporary
    file, never a URL or an attachment path supplied by a caller.
    """

    def __init__(
        self, *, ffmpeg: str = "ffmpeg", ffprobe: str = "ffprobe",
        sample_positions: Sequence[float] = _SAMPLE_POSITIONS,
    ) -> None:
        positions = tuple(sample_positions)
        if not (
            1 <= len(positions) <= _MAX_SAMPLE_COUNT
            and all(isinstance(position, (int, float)) and not isinstance(position, bool)
                    and 0 <= position < 1 for position in positions)
            and all(left < right for left, right in zip(positions, positions[1:]))
        ):
            raise ValueError("Video sample positions must contain 1–12 increasing fractions in [0, 1).")
        self._ffmpeg = ffmpeg
        self._ffprobe = ffprobe
        self._sample_positions = positions
        self._decode_lock = asyncio.Lock()

    async def extract(self, content: bytes, mime_type: str) -> tuple[VideoFrame, ...]:
        if mime_type != "video/mp4" or not isinstance(content, bytes) or not 0 < len(content) <= MAX_VIDEO_PREVIEW_BYTES:
            return ()
        try:
            async with asyncio.timeout(12):
                async with self._decode_lock:
                    with TemporaryDirectory(prefix="openagent-video-") as directory:
                        path = Path(directory) / "attachment.mp4"
                        path.write_bytes(content)
                        return await self._sample(str(path))
        except (OSError, TimeoutError):
            return ()

    async def _sample(self, path: str) -> tuple[VideoFrame, ...]:
        probe = await _bounded_output(
            self._ffprobe,
            ("-v", "error", "-max_alloc", _MAX_ALLOCATION, *_LOCAL_MP4,
             "-i", path, "-select_streams", "V:0",
             "-show_entries", "stream=width,height,duration:format=duration",
             "-of", "json"),
            8 * 1024,
        )
        try:
            metadata = json.loads(probe)
            stream = metadata["streams"][0]
            duration = float(stream.get("duration") or metadata["format"]["duration"])
            width, height = int(stream["width"]), int(stream["height"])
        except (KeyError, IndexError, ValueError, TypeError, AttributeError):
            return ()
        if not (math.isfinite(duration) and 0 < duration <= _MAX_DURATION
                and 0 < width <= 4096 and 0 < height <= 4096 and width * height <= _MAX_PIXELS):
            return ()

        frames = []
        for position in self._sample_positions:
            timestamp = duration * position
            jpeg = await _bounded_output(
                self._ffmpeg,
                ("-v", "error", "-nostdin", "-max_alloc", _MAX_ALLOCATION, *_LOCAL_MP4,
                 "-ss", f"{timestamp:.6f}", "-threads", "1",
                 "-max_pixels", str(_MAX_PIXELS), "-i", path, "-map", "0:V:0",
                 "-filter_threads", "1", "-frames:v", "1", "-an", "-sn", "-dn",
                 "-vf", f"scale={_FRAME_EDGE}:{_FRAME_EDGE}:force_original_aspect_ratio=decrease:force_divisible_by=2",
                 "-threads", "1", "-c:v", "mjpeg", "-f", "image2pipe", "pipe:1"),
                _MAX_FRAME_BYTES,
            )
            if jpeg.startswith(b"\xff\xd8") and jpeg.endswith(b"\xff\xd9"):
                frames.append(VideoFrame(jpeg, timestamp))
        return tuple(frames)
