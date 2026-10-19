# Core b23 attachment and child-identity regression evidence

Source baseline: `268062f0b8833f86bdb84e5fd5ddd990ef885369` (official b22).
Changes live in Core; no host compensation or new runtime dependency is required.

The targeted deterministic suite passed **59/59** in 1.25 seconds on Python 3.11,
with one test worker and no provider inference, service restart or release build.
It ran the generic attachments, bounded child IDs, proxy capabilities,
engine executor and runtime effects public test files. Original-source test log
SHA-256: `71cb9aa0eb04b510bd8bf7a5dd1d79483ce4606bee0c3f272f9a7601f93cb578`.

Coverage includes:

- Two real synthetic XLSX files under correct and generic MIME, text-only,
  Codex-style and Claude-style catalog declarations; multi-sheet/shared strings,
  cached/uncached formulas and original hash preservation.
- Unknown DOCX/ZIP/binary files remain host references without binary provider
  payloads, automatic archive extraction or network fetches. Unicode filenames,
  escaped metadata, host IDs and local paths survive routing.
- ZIP/XML expansion, structure, path and output bounds; corrupt/unsafe workbooks
  report a rejected preview while retaining originals. Preview limits never
  suppress references to otherwise admitted files within the documented batch.
- Actual dispatcher generate/stream capture; model-native image/audio/video
  preference and text-only reference fallback; existing native PDF routing.
- Bounded IDs through 32 nested hops and the real delegation handler through
  three levels, preserving exact parent/model/owner metadata.
- Existing engine terminal-failure and runtime authorization/effects cases.

Earlier test attempts are retained privately: one fixture used a model label
with trailing whitespace already normalized by the handler; another caught an
MIME wildcard admission edge and was corrected before this final passing run.

Not established by these tests: live model comprehension of the spreadsheet,
any product UI/installer, new OAuth enrollment, native device or production
cutover. A host must provide authorized readers and retain original files for
their required lifetime. Reference fallback is not a grant or a promise that an
unsupported binary has been read.
