# Managed attachment privacy

`ManagedAttachmentSnapshots` is an explicit host API for private copies owned by
one server writer. It does not make `send_file_to_user` or arbitrary filesystem
paths privacy qualified. The ordinary marker tool refuses a protected-origin
execution before inspecting a path or returning a delivery marker.

The host creates an exclusive snapshot directory and supplies the canonical
`ContentPrivacyStore` and an authorized export callback. It supplies a verified
`ContentRevision`, exact bytes, trusted origins or immutable parent revision
references, and `wholly_attributed=True` to `write`. A complete customer fragment
or wholly generated summary can be published this way; a raw file mixing human
or different customers needs a host-verified partition first. Model arguments,
filenames and recent session activity do not establish provenance.

The writer flushes an immutable metadata pin before recording the revision.
`ledger.publish` rechecks the revocation fence inside the serialized transaction
before writing any payload. The staged payload is flushed with a writable handle
and renamed. The same lock protects publication and the sink. A new instance can
recover its pins after the previous writer has stopped; two writer processes
must not own this directory concurrently.

The sink exports through the host callback and removes only exact, bounded,
regular snapshot bytes, including a complete staged copy left before rename.
Human edits, symlinks, corrupt pins and partial staged bytes produce a conflict;
they are preserved for verified recovery and never reported as completed.
Replaying a completed redaction is idempotent, and its origin remains fenced.

Only these managed copies are covered. Channel uploads, external recipients,
arbitrary source files, the Vault, Git history/reflogs/remotes and other legacy
copies need separate qualified sinks. Declaring only `attachments` as covered
leaves the overall privacy job blocked before sink work. Do not set a capability's
`privacy_fenced=True` merely because this helper exists.

Vault service mutations and the attachment marker tool have direct pre-IO gates
in addition to catalog admission. Ordinary human management outside an admitted
run does not inherit protected origins from audit trailers. Existing legacy
content cannot be retrospectively attributed from timestamps: its outstanding
privacy job stays blocked until the host provides verified revision coverage.
