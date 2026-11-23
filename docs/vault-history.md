# Vault history

Every vault change is a Git commit. Writes through the vault service commit
exactly their paths with provenance; a background sweep (`commit_all`) commits
edits made by other writers. Agents are told nothing in the vault is lost, so a
commit failure must never be silent.

A Git process that dies mid-operation leaves `.git/index.lock` behind, after
which every `git add` fails. The service treats a lock older than ten minutes
as abandoned, removes it once and retries; a younger lock is kept, because a
live Git may own it. Any failed `git add` or `git commit` is logged at warning
level instead of being reported as "nothing to commit". One installation had
four months of vault edits left uncommitted behind such a lock.

`vault_validate_note` resolves wikilinks against the search index. When a link
does not resolve, the index is synchronised incrementally once before the link
is reported as broken, so a note written a moment earlier through another
writer is recognised.

Regressions: `tests/public/test_vault_git_stale_lock.py`.

## Importing a Git vault into a document space

The administrative import reads a host-fenced state snapshot. Supply the relative vault directory, a canonical SQLite database with tenant evidence, and an explicit list of note paths whose entire history the space may read. Include former names of renamed or deleted notes when their history belongs to the same audience. A filename pattern or the account that happens to be logged in cannot establish that ownership. Keep the original snapshot in private backup custody with its original retention policy.

Initialize the candidate's `SqlDocumentVault` and hold ordinary writers and maintenance offline. Grant the migration principal `memory.import` on its host-selected space and `memory.publish` for its audience. The same API accepts a vault backed by SQLite or PostgreSQL.

```python
from openagent_storage_sqlite.vault_git_source import VaultSource
from openagent_storage_sqlite.vault_import import import_vault, verify_import, restore_vault

source = VaultSource(snapshot, vault_directory, approved_historical_paths)
inventory = await import_vault(document_vault, host_context, source, canonical_database)
verified = await verify_import(document_vault, host_context, source, canonical_database)
```

Each scope starts empty. Transfer writes private chunks that can be resumed with the same source, scope and corpus. A transaction checks the staged digest and publishes the entire space. Verification independently reads the Git revisions, note bodies, authorship, links, current titles and SQLite search rows. Returned receipts contain counts and digests. Keep the migration database role and inbox inaccessible to product clients.

The import enumerates commits reachable from refs or reflogs in topological order. It records changes against each commit's first parent, records all parent hashes, retains rename peers within the authorized corpus and appends the observed working tree. This preserves divergent branch versions, untracked notes, deletions and line endings. Exact trees, full commit messages, signatures and original refs remain in the sealed Git archive. A source Git author or origin trailer is historical metadata. The host authorizer makes the access decision. Inspect an imported revision's facts through `document_vault.revision_origin(context, path, revision=n)`. Native document writes after import retain their authenticated runtime authorship.

Before enabling the candidate, exercise restore with the host's writer fence and a new, private destination directory. Include the complete set of newly created document paths that have an attested filesystem destination.

```python
restored = await restore_vault(
    document_vault, host_context, source, canonical_database, new_directory,
    new_paths=approved_new_paths, quiesce=host_writer_fence,
)
```

The drill restores only the original vault subtree, including its Git objects, refs, index and untracked regular files. It overlays subsequent document writes, validates current bytes, and writes a private `post-import-revisions.jsonl` beside the state tree with each later revision, authenticated author and recorded session/run. The original Git refs stay anchored to the captured commits. The exported later revisions remain available for separate Git or SQL replay. Use only a drill with a `complete.json` receipt. A forgotten historical note requires privacy-qualified Git erasure before the old archive can be served again. Cross-domain exports, full application rollback, run/channel state, credentials and activation require their respective host procedures.

For a standalone host using the same SQLite database for documents and sessions, call `restore_standalone_memory_and_sessions` with the same arguments. Keep all session, attachment, document and maintenance writers fenced until it returns. It additionally checks the database installation identity, runs SQLite online backup with the current WAL, verifies integrity and canonical row totals, and copies each referenced available attachment to its original content address after verifying length and SHA-256. This includes sessions, replies, attachments and document changes accepted after activation. The rollback target is a new directory. Select it only after its `state-complete.json` receipt exists and the original application can reopen it. Configure the file-backed vault on return and keep the same runtime identity, directory mapping and credentials. Other application state keeps the host's existing mapping.

A run or delegated child that has consumed a document may expose its output only while its source revision and audience remain authorized. The standalone return refuses any retained run-to-document binding, including sources that are still readable at the cut. Earlier binaries cannot apply a future revocation to the chat, event or model history they serve. On this refusal the operator keeps the document-aware runtime and its SQL store active. An independently sanitized export would need to cover all affected projections before a legacy reader could serve that cohort.

### Desktop revision operations on a document space

The native history list, revision detail, unified diff and restore share the scoped SQL journal. Every accepted edit, deletion and restoration records a space-local revision. Imported Git identities keep their first-parent ancestry across branches, with the captured working tree as the initial writable head. An installation upgrading an older SQL document store without a global journal starts with an explicitly labelled snapshot of its current corpus. Existing note revisions stay available through the note history API.

Restore appends a new commit from the selected state. The entire change and the final management authorization check share one database transaction. An explicitly confirmed reset removes a tail of native commits from the same space and preserves monotonic note revisions. The original imported Git graph remains in custody. A reset across that boundary returns a conflict. Use restore to select an earlier Git state. A forget removes the note's version bodies, provenance and entries from the visible journal. A private filesystem rollback after ordinary SQL deletion records the deletion and removes the file only from the newly restored tree.


For a sealed vault whose source never contained a Git repository, use
`VaultSource(snapshot, relative_vault, approved_paths, working_tree_only=True)`.
Each file carries its exact manifest hash and working-tree provenance. This mode
rejects an existing Git directory. An empty selection requires an empty Markdown
inventory. A restored candidate also contains the virtual scaffold folders created
in SQL. If the source included a generated `llms.txt` and the space owns it,
include that exact path in the selection before enabling SQL regeneration.

When the source genuinely has no vault directory, seal it with
`prepare_migration(..., absent_vault=relative_vault)` and select
`VaultSource(snapshot, relative_vault, [], absent_vault=True)`. The importer
requires the exact absence receipt and an empty document selection. Existing
sessions retain their original tenant checks. A fresh installation with an empty
canonical session table may establish this empty space using its offline host
grant. The receipt enables subsequent SQL writes without inventing prior notes.
