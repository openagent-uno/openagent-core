# Changing MCP sources while the agent runs

Adding, editing, enabling, disabling or removing a source (REST or the
`mcp-manager` tools) updates the registry and the live pool at once:

- **Only the changed source restarts.** `MCPPool.reload()` compares each
  resolved spec with the running one. A source whose spec is identical keeps
  its connection and toolkit, so its catalog registration and every tool
  reference already issued stay valid. A dormant source (connected, zero
  tools) is retried. A changed source is replaced; a removed one is closed.
- **Discovery changes from the next turn.** Each run keeps the capability
  snapshot it was admitted with (`CapabilityCatalog.snapshot_revision()`).
  A new source is registered immediately but becomes discoverable when the
  next run starts; a removed or disabled one stays usable to runs that already
  hold it until their registration is revoked. The `mcp-manager` mutation
  tools say so in their answer: `"availability": "next_turn"` with a `note`.
- **The answer is the real outcome.** Registry writes (`upsert_mcp`,
  `set_mcp_enabled`, `delete_mcp`) run in a dedicated short transaction and
  `upsert_mcp` returns the row as committed. On the shared connection a
  COMMIT fails with "SQL statements in progress" while a sibling coroutine
  (for example a running turn numbering its messages with
  `INSERT ... RETURNING`) holds an unfinished statement, and the pending
  insert was then committed by that sibling: the caller reported a failure for
  a write that had happened.
