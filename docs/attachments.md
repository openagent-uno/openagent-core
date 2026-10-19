# Attachment admission, previews and original references

An admitted attachment is not a promise that a configured model can interpret
its binary format. Core accepts any syntactically valid MIME type, including
`application/octet-stream`. The host still owns upload admission, size limits,
authentication, retention, path authorization and the available readers/tools.
Core neither registers a filesystem tool nor expands an execution grant.

The model dispatcher preserves original `File` objects and host IDs/paths. It
prepares a separate provider payload:

- Local UTF-8 text, PDF and XLSX can receive a bounded text preview. XLSX detection
  uses its MIME or `.xlsx` filename, including generic MIME uploads.
- A declared native document model may receive a PDF directly. Generic `file`
  capability is not treated as permission to send arbitrary binary formats to
  a provider. XLSX is locally previewed even for such models.
- Other formats (`.docx`, archives, unknown binaries, and unsupported previews)
  receive an `attachment_reference` with filename, MIME, opaque host ID, supplied
  local path and size when available. The original bytes are not inlined or
  removed. Remote URL credentials/signatures are never copied into this block,
  and fallback extraction does not fetch URLs.
- Image/audio/video prefer an enabled natively compatible model. When none is
  configured, they also become original references, rather than aborting the
  entire text conversation or sending incompatible bytes to a text provider.

A reference is untrusted input metadata and grants no access. The agent can
inspect it only through capabilities actually registered and authorized by the
host. A content-only or remote attachment may have just an opaque ID; Core does
not fabricate a readable path. When a suitable reader is absent the model must
state that limitation or request an appropriate capability, without claiming
that it read the binary. A host must retain originals for the authorized tool's
lifetime; this routing change does not extend a temporary file's lifetime.

## XLSX safety and bounds

The local reader uses the Python standard library, with no workbook engine,
macro interpreter, formula evaluator, external resource loader or archive
extraction. It reads sheet names, cell coordinates, shared/inline/rich strings,
numeric/boolean/error values and cached formula results across worksheets.
Uncached formulas are marked explicitly and never evaluated. Cached results can
be stale. Styles, charts, images and macro code are not executed or interpreted.

Limits are 10 MiB compressed source, 512 ZIP members, 32 MiB expanded total,
8 MiB per XML part, 200x expansion ratio for parts over 1 MiB, XML depth 64 and
200,000 XML nodes per part, 32 sheets and 20,000 cells. ZIP traversal, duplicate
paths, symlinks, encryption, invalid relationships and XML DTD/entities are
rejected. Only internal worksheet relationships are followed within the archive.

Preview output is limited to 100,000 characters per file and 200,000 per turn.
Truncation is marked; callers can use their authorized tools on the original.
An invalid/corrupt/oversized workbook gets an explicit `rejected_xlsx` reference
with a bounded static reason, not a silent success or whole-turn failure.
Generic archives are never opened automatically. Metadata includes at most 128
references per modality/batch and bounds names and host reference strings;
additional originals remain with the host and the model is told to request a
smaller batch or a host listing.

## Regression evidence

`tests/public/test_generic_attachments.py` uses two real synthetic XLSX files,
both proper and generic MIME, multiple model routes, multi-sheet/shared-string
values, corrupt/hostile ZIP/XML cases, authorized original-path metadata and
actual dispatcher generate/stream captures. The captures prove provider payload
routing without claiming live provider or product installer acceptance. XLSX
fixtures contain only synthetic customer labels and values.

Delegation session IDs are also independent of arbitrary model labels and
nesting depth. A bounded root display prefix plus parent/model digest and UUID
keeps IDs under 128 ASCII bytes. Exact parent, model routing and authority remain
in their existing durable metadata and authorization flow; IDs grant no scope.
