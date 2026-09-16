# Capability host

Optional engine-free discovery and execution host. Construction receives explicit paths and capabilities, does not create files, and installs no built-in tools. Plugin loading is disabled unless the host product enables it. Per-principal durable idempotency, admission/revocation barriers, leases and lossless results retain the existing implementation.

Extracted from openagent-host-tools af6ad6871d4d1208874bf79735710d089f59b959. Public tool value types are provided by the independent openagent-tool-protocol package; there is no dependency on the runtime or concrete tools.
