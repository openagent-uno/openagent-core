"""Lightweight shared response contract for stream consumers."""
from dataclasses import dataclass, field

@dataclass
class BatchedReply:
    """Result of a :meth:`BatchedChannel.run_one_shot` call.

    Mirrors the shape of legacy ``TurnRunner.run`` return value so
    bridges can keep their existing render path.
    """

    text: str = ""
    audio_chunks: list[bytes] = field(default_factory=list)
    audio_format: str | None = None
    audio_mime: str | None = None
    voice_id: str | None = None
    attachments: list[dict] = field(default_factory=list)
    parts: list[dict] = field(default_factory=list)
    model: str | None = None
    errored: bool = False
    error_text: str | None = None

    @property
    def audio_bytes(self) -> bytes | None:
        if not self.audio_chunks:
            return None
        return b"".join(self.audio_chunks)

