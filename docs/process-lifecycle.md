# Host process ownership

Core does not manage a host application's server process, display or client
topology. In 1.1.0b51 the legacy `core.serve_singleton` process scan and termination
functions are deprecated compatibility no-ops. No command-line substring, process
name or directory basename proves that a process is stale or belongs to a host.

An application requiring one process per installation should hold an exclusive
filesystem lock throughout its own lifetime, refuse a concurrent start, and use
its service manager for an intentional restart. Core itself remains usable in
embedded runtimes without any process singleton, installation directory or client.

Native regression qualification starts real owned fixtures with both desktop
launcher and legacy server-looking arguments and verifies that compatibility
calls cannot signal them. The fixture owner alone retires its children afterward.
