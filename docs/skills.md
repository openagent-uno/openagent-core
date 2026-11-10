# Skills

OpenAgent Core owns installed skill search and lifecycle. The agent uses
`skill_search`, `skill_view` and `skill_manage`. Hosts can expose the same
operations through `openagent_core.workspace_administration.SkillsLibrary`
using their verified identity and `skills.read` / `skills.write` policy.
Standalone adapters with their own authenticated boundary can delegate to
the same skill handlers. Only trusted management callers set
`human_authored=True` on the write handler. Agent tools do not accept it.

Search includes the name, category, description and Markdown instructions.
A configured embedder also supports semantic matching. Agents search active
skills. Hosts can opt into searching archived entries for library management.
Search and explicit reads see a saved skill immediately. The cached index in
the system prompt refreshes on agent boot or reload.

New skills record whether a person or agent wrote them. Edits retain other
frontmatter fields such as source, license and pin. An authorized external
channel agent can create skills and update or archive unpinned agent-authored
skills. Proposal-only jobs cannot write, and channel agents use archive for
retirement. The embedding host controls channel access to the skill library.

## Published catalogs

Enable `skills.hub.enabled` and set `skills.hub.taps` to the Git catalogs the
host wants to expose. A catalog contains skill directories at its root.
`skill_hub_search` searches configured catalogs without installing anything.
It returns the source, folder, commit and scan verdict. A foreground agent
can pass the source, folder and commit to `skill_hub_pull` to import a
selected skill. A channel agent can import an individual revision from a
configured catalog with its normal scan policy. If the source revision
changes, search again. The importer
quarantines and scans the contents and records the source and commit. Git
taps are selected by the host. There is no built-in universal registry.
