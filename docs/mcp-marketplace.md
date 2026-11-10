# MCP marketplace in agent sessions

Hosts can register `MarketplaceCapability` to expose the official MCP
Registry, including entries from third-party publishers. A run searches by
name, follows the registry cursor, inspects a version and selects a transport.
Installation refetches the exact version and requires the digest shown by
inspection. The result includes a credential-free registry link suitable for
a conversation. Registry entries describe install options. Publication in the
registry does not grant runtime permissions.

An embedding host implements `MarketplaceInstaller` to advertise the
transport options it can support and execute a host-owned installation
transaction. Search and inspection use `catalog.list`. Installation requires
`catalog.install` on `ResourceRef("mcp", tenant, "mcp-marketplace")` for the
exact audience. Use a separate host credential flow for secret fields.
Installers own isolation, grants, connection readiness and idempotency.

Hosts using `MCPPool.from_db` may opt into the public HTTPS transport by
setting `_public_https_only` in the trusted host spec resolver. That transport
limits requests to the exact HTTPS URL, validates public IPv4 DNS answers,
pins the address for each request and verifies TLS against the original
hostname. Redirects, embedded credentials, private hosts and ambient proxies
are excluded. Configure a dedicated execution boundary for stdio package
recipes before advertising them to an unattended agent.

[Official registry API](https://github.com/modelcontextprotocol/registry/blob/main/docs/reference/api/official-registry-api.md)
