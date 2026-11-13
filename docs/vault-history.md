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
