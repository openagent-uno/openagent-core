# Agent activation of reviewed MCP integrations

An embedding host may register the optional managed capability from
`openagent_core.mcp.reviewed_installations`. Bind
`ReviewedMCPInstallationCapability(installer)` as both source and executor
in the runtime capability catalog. The host supplies a `ReviewedMCPInstaller`
with an inventory and an activation transaction. Existing fixed, managed and
dynamic MCP profiles retain their current administration behavior.

The model receives the title, description, opaque ID and exact revision of
eligible offers. The two tools are `list_reviewed_mcps` and
`install_reviewed_mcp`. The latter accepts only the ID and revision. The
model cannot supply an endpoint, program, environment or credential through
this capability. The normal catalog enforces tool authority. The runtime also
requires `catalog.list` on the `mcp` resource `reviewed-mcps` and
`catalog.install` on the exact offer ID for the current tenant and audience.

The host owns review, scope, installation and transport. Its installer must
revalidate the exact revision and permission at the mutation boundary. Repeat
calls for the same tenant, agent and revision must converge on one installation.
Conflicting existing configurations require review. Newly registered tools
become available on the next admitted run under the normal catalog snapshot.
A successful source test establishes this contract only. Installation and use
against a real provider require the host's own acceptance evidence.
