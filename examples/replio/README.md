# Replio reference consumer

This installable example embeds the actual OpenAgent engine. It demonstrates
the integration contract for a product with its own authentication, session
membership and fixed project tools. It does not migrate the real Replio app.

`ReplioHost` constructs a `Runtime` from public `Agent`, `AgentExecutor`,
`NativeProvider`, `CapabilityCatalog` and `SqliteRuntimeStore` APIs. Each host
instance owns one Replio tenant/project and its data directory. Shared sessions
may contain several Replio users; every message is admitted with its own author
and authorization context. The host must supply the session membership and an
`IdentityAdapter` backed by Replio's existing authentication service. OpenAgent
creates no accounts, passwords, primary owner or synthetic identity.

The two managed tools list and read this project's documents. They cannot change
tenant, project, destination or principal through arguments. There is no dynamic
MCP installation, shell, filesystem, browser, PTC, computer control, dashboard or
catalog manager. Direct public catalog installation/removal also rejects these
operations. The model sees the common opaque tool reference contract and mandatory
framework prompt. This example deliberately selects no vault module; the
standalone and GlassPalace presets keep their own required vault modules.

The optional `create_app(host)` HTTP adapter validates the external bearer
identity and current membership before building a context. Its routes are:

| Route | Behavior |
| --- | --- |
| `POST /sessions/{session}/runs` | Admit `run_id`, `idempotency_key`, `input`. |
| `GET /sessions/{session}/runs/{run}` | Read authorized durable state. |
| `GET /sessions/{session}/runs/{run}/events?after=N` | Replay authorized events. |
| `POST /sessions/{session}/runs/{run}/cancel` | Cancel the exact run. |
| `GET /sessions/{session}/tools` | Discover the currently authorized fixed tools. |
| `POST` or `DELETE` on the tool catalog | Deny mutation. |

To embed it, construct `ReplioHost` with explicit identity, project membership,
documents, data directory, provider URL/model/key and optional system prompt.
Pass the resulting app to your HTTP host, or use `host.runtime` directly with
contexts returned by `host.policy.context`. Close the app/runtime during host
shutdown. The API never accepts identity or capabilities in caller JSON.

Build this example independently with:

```sh
uv build --wheel --out-dir /absolute/wheelhouse examples/replio
```

Install the wheel with matching `openagent-core` and `openagent-storage-sqlite`
`1.1.0b1` wheels, then run the installed-package verifier from any directory:

```sh
uv pip install --python /absolute/venv/bin/python --prerelease=allow --find-links /absolute/wheelhouse replio-agent-example==1.1.0b1
/absolute/venv/bin/python /absolute/openagent-core/examples/replio/scripts/verify-installed.py
```

The verifier checks all three imports resolve inside the environment's installed
packages and copies the tests to a temporary directory before running them.
Tests use authenticated HTTP at both the application and model boundaries, the
real native provider/tool loop, SQLite and real tool discovery/calls. The model
endpoint is deterministic, so these checks consume no paid provider requests.

Coverage includes two authors in one persisted session, restart/idempotency,
author conflicts, replay, a different tenant, a private session, forged identity,
managed catalog mutation, argument manipulation and current revocation. This is
not evidence of authentication against the real Replio service, commercial model
quality, browser/native UI, production deployment or a signed distribution.
