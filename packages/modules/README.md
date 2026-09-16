# OpenAgent module resources

Optional resources for modules selected explicitly by the embedding host. The
minimal `openagent-core` install does not include or require this package.

`module_assets("vault")` returns the installed vault resource directory. The
host provides a Node.js 20+ executable, a vault directory, and authorization;
module resolution never installs dependencies or compiles code.

The vault resource is built from the preserved TypeScript implementation in
`packages/runtime/src/openagent_core/mcp/servers/vault`. Run `npm ci` there and
`node scripts/build-module-assets.mjs` from the repository root before building
this wheel. Its manifest records the bundled artifact hashes. Dependency license
notices and the unchanged third-party system-trash helpers are included.

Only deterministic stdio and local-vault behavior has been qualified here.
System-trash integration and signed product distributions require platform E2E.
