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
