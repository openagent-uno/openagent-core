# Historical engine fixtures

These files retain the pre-extraction server test history. The installed public
contract suite is `tests/public`; the applicable prompt/vault baseline and
its real compiled-module qualification are recorded in
`docs/migration/vault-tool-parity.md`. The old all-in-one server driver is not
the v1 acceptance entrypoint.

Product support tests and their replay helpers now live with the preserved
component history in `openagent/apps/server/scripts`. Run the product
`tests/verify_support.py` driver: it exercises the public Runtime/catalog using
SQLite fixture identities. The migration comparison against original server
1842ce6 records 194 passing cases and the exact same 36 pre-existing failures
across 230 cases, including matching failure-message hashes. Those product
test copies are removed from core; core does not import the support extension.
