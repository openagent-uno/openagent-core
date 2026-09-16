# Vault administration for embedding hosts

`openagent_core.vault_administration.VaultAdministration` exposes the same vault
service to standalone and embedded management interfaces. A host supplies an
explicit corpus path, an authorizer and either its runtime or an explicit index
path. Construction performs no I/O. Runtime-backed instances borrow the runtime's
vault service; independent instances own their service and must be closed.

Call `execute(context, operation, path=..., query=..., body=...)` with a verified
`ManagementContext`. The service checks current `vault.read` or `vault.write`
permission on the exact agent before accessing the corpus and again before
returning. The host decides whether this corpus is private or shared. Results
are returned only to the management caller; publishing memory into a conversation
continues to require the separate runtime memory and audience contracts.

Supported operations are note listing/read/write/delete, graph, full-text/file/
in-file search, gate, stats, history, commit detail, restore, reset, doctor,
derived artifacts, move, taxonomy initialization and index synchronization.
`reset` requires `confirm: true`; it is distinct from non-destructive restore.
Paths must be relative Markdown paths without hidden components or symlinks.
Whole-corpus mutations reject symlinks and the index excludes them. Regex search
has a two-second deadline, a result bound and bounded returned lines.

Writes go through the shared validation service. Rejection or validator failure
never falls back to a raw file write. The host must preserve the submitted editor
text when displaying these failures. Provenance uses the verified principal key,
not request-supplied origin fields. Git is initialized before the first mutation
so its baseline cannot swallow that mutation's author. Maintenance commits its
own changes under the vault mutation lock; dream calls pass their trusted origin
through both repairs and generated artifacts. Empty scaffold directories are
excluded from Git pathspecs so they cannot prevent template commits.

Verification on temporary data includes five public service tests, three
authenticated GlassPalace HTTP tests and the existing 130 vault/tool parity
cases. The historical checks now target the public service for graph, path and
frontmatter behavior. Commit-count expectations explicitly include the initial
baseline and separately verify the first note's provenance. These tests do not
qualify a real provider or deployment.
