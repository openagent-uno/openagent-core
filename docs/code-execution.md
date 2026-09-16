# Host-owned code execution

`RuntimeServices.code_executor` accepts a structural implementation of the public
`openagent_core.code_execution.CodeExecutor` protocol. It defaults to `None`.
PTC refuses execution when the service is absent, the selected transport is
unsupported, or `require_sandbox` is true and the host has not attested isolation.
An unavailable executor never switches to a different machine or environment.

The consumer explicitly supplies the interpreter, environment, isolation policy
and either a Unix socket bridge or a filesystem bridge. Core retains its random
per-run token, opaque tool references, call budget, allowlist, dry-run and exact
execution/ingress context. Each tool call rechecks current authorization through
the same catalog as ordinary tool dispatch. Temporary files are removed at the
end of the call; the host owns executor lifecycle and infrastructure cleanup.

`openagent-execution` in the tools repository is an independent implementation
backed by `openagent-shell`. It has no engine dependency. OpenAgent standalone
composes its explicitly selected local or Docker backend; GlassPalace supplies
its already provisioned dedicated agent pod and forwards no provider or workload
credentials. Replio does not enable PTC or provide an executor.

The old core shell, device, editor, web-search, dashboard, agent-manager and
federation implementations have been removed. Tool process code and native
sidecars have one active home in `openagent-tools`; dashboard and product
identity/federation services have one active home in the standalone product.

`tests/public/test_ptc_executor.py` uses real Python subprocesses and the public
catalog through both bridge transports, covering full results, context,
revocation, dry-run, budgets, environment isolation, refusal and timeout.
The filesystem bridge test uses a local fixture: it does not qualify Docker,
SSH, Kubernetes, Windows, signed installers or authenticated product UI flows.
