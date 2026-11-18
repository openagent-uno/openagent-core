# Fresh operational ACL batches

`openagent_core.memory.operational.access.resources_are_visible(connection,
rows, access, permission="view")` is the asynchronous batch equivalent of
`resource_is_visible`. It returns one boolean for every input row, in the same
order, including duplicate resource IDs or different versions of the same ID.
Supply the same verified `AccessContext` accepted by the scalar reader. There
is no implicit trusted-service mode: `None` does not authorize a resource.

The canonical rules are unchanged: exact tenant, quarantine denial, typed
ownership aliases, public/installation-shared read access, and explicit typed
grants at the row's exact ACL version. A public resource does not acquire admin
permission. Grant IDs, principal types and opaque resource identifiers are SQL
parameters, without JSON conversion, prefix matching or interpolation.

Queries contain at most 200 resources and 64 principal aliases, keeping their
930 bound parameters below SQLite's conservative 999-variable limit. Additional
resources and aliases are checked in further bounded queries. The function
neither caches decisions nor changes the caller's transaction, storage, history
or connection lifecycle. Database errors and cancellation propagate.

Call the batch reader after fetching candidate rows, then recheck the selected
page before publishing it after awaited work. Each query observes the caller
connection's transaction snapshot, just as the scalar reader does; callers
remain responsible for their transaction and publication boundaries.

This API covers only the existing operational database ACL. It does not replace
`Runtime` or host authorization, device/principal liveness and epochs, external
grant/lease expiry, resource lifecycle checks, privacy fences, or cursor expiry
and identity checks. The canonical `resource_acl` schema has no expiry field.
A host with additional policy must continue to authorize before reading and
again before publication, even when the database batch returns `True`.

`tests/public/test_operational_acl_batch.py` compares batch and scalar decisions
over tenant/visibility/owner/version/permission combinations and typed aliases.
It also covers fresh revocation and regrant, opaque IDs, ordering and duplicates,
bounded queries, explicit service contexts, caller-owned transactions, failures,
separate-writer revocation after row fetch, and host expiry/epoch/custom-policy
denial after a successful database ACL read.
