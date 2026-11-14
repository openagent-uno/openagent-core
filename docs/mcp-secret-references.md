# Host secret references in MCP sources

A remote MCP source often needs a credential in a request header
(`Authorization: Bearer ...`), and a local (stdio) source in its environment.
Writing the credential into the source configuration stores it in the catalog
and shows it to whoever reads the configuration. Instead, a header or stdio
environment value can reference a secret held by the server process:

```
${secret:OPENAGENT_MCP_SECRET_<NAME>}
```

When the source connects, each reference is replaced by the value of that
variable in the server process environment. Only names matching
`OPENAGENT_MCP_SECRET_[A-Z0-9_]+` can be referenced. The operator opts a value in
by giving it that name; no other process variable (model or database
credentials, for example) can be sent to a URL chosen by whoever configures a
source.

If a reference uses another name, or the variable is missing or empty, the
source does not start. Its `connection_status()` entry is `unavailable` with
failure `{"code": "secret_unavailable", "phase": "configure"}`, and the server
log names the reference (never a value).

The stored configuration keeps the reference. The value is resolved only into
the transport parameters, so it does not appear in the catalog, status
snapshots, logs, catalog summaries or model context. The `mcp-manager` tools
(`add_custom_mcp`, `update_mcp`) accept `headers`; their answers, and
`get_mcp` / `list_mcps`, show header values masked as `***` unless the value is
only references with an optional scheme such as `Bearer`.

## Example

Keep the token in a Kubernetes Secret and expose it to the server container:

```yaml
env:
  - name: OPENAGENT_MCP_SECRET_EXAMPLE_TOKEN
    valueFrom:
      secretKeyRef:
        name: example-mcp
        key: token
```

Then add the source (from the app, the HTTP API or the `mcp-manager` tools):

```json
{
  "name": "example",
  "url": "https://mcp.example.com/mcp",
  "headers": {"Authorization": "Bearer ${secret:OPENAGENT_MCP_SECRET_EXAMPLE_TOKEN}"}
}
```

Rotating the secret only needs a restart of the server process; the source
configuration does not change. Regressions are in
`tests/public/test_mcp_secret_references.py`.
