# Core beta24 attachment presentation and delivery

Source regressions on 19 October 2026: 15 tests pass without skips in
`tests/public/test_workspace_administration.py` (10) and
`tests/public/test_engine_executor.py` (5). The host runs these serially with
existing dependencies; this evidence does not claim installed-wheel, public
release, live-provider or product UI qualification.

The tests execute a real Stream turn with separately prepared provider input
and verify canonical original text and CAS files in history. An AgentExecutor
turn invokes the standard attachment tool, an explicit host publisher and real
ArtifactRepository upload, then verifies CSV bytes, output linkage, ordered
live/history parts, tenant/session isolation, deletion and fresh authorization.
The terminal attachment survives the bounded 2,000-event live journal window.
The provider is deterministic; it is not a paid or real-model acceptance test.

File/image/audio/video references are tested for both roles, with interleaved
text preserved. System/history/tool slots and two run identities cannot overlay
one another. Literal attachment-reference and memory-checkpoint user text is
retained. No public local path, binary body or caller URL is exposed.

A Runtime without the host publisher cannot stat a sender path. Missing,
forbidden or invalid output delivery preserves valid answer text with a static
unavailable notice; publication revocation emits neither text nor references.
The full attachment and host-boundary contract is in [attachments](../attachments.md).
