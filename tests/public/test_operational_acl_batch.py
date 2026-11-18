"""Batch ACL reads preserve scalar decisions, freshness and host boundaries."""
import asyncio
from dataclasses import replace
from itertools import product
from pathlib import Path
from types import SimpleNamespace
import sqlite3
import tempfile
import unittest

import aiosqlite

from openagent_core import ExecutionContext, PrincipalRef, ResourceRef
from openagent_core.contracts import require_authorized
from openagent_core.memory.operational.access import (
    AccessContext, resource_is_visible, resources_are_visible,
)
from openagent_core.memory.operational.schema import operational_schema_sql


def access(kind="user", handle="reader", tenant="tenant"):
    return AccessContext.from_request({
        "device_cert": SimpleNamespace(network_id=tenant, handle=handle, device_pubkey_hex="device",
                                       capabilities=["agent"] if kind == "agent" else []),
        "network_id": tenant, "user_handle": handle, "client_id": "device",
    })


def row(identifier="resource", **changes):
    return {"tenant_id": "tenant", "resource_type": "session", "resource_id": identifier,
            "visibility": "private", "owner_principal_id": "user:other", "acl_version": 1, **changes}


class OperationalAclBatchTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.conn = await aiosqlite.connect(":memory:")
        self.addAsyncCleanup(self.conn.close)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.executescript(operational_schema_sql())
        await self.conn.commit()

    async def grant(self, identifier, *, tenant="tenant", resource_type="session", principal_type="user",
                    principal_id="reader", permission="view", version=1):
        await self.conn.execute("""INSERT OR REPLACE INTO resource_acl
            (tenant_id,resource_type,resource_id,principal_type,principal_id,permission,acl_version,granted_at_ms)
            VALUES (?,?,?,?,?,?,?,1)""", (tenant, resource_type, identifier, principal_type, principal_id, permission, version))

    async def assert_scalar_parity(self, rows, context=None, permission="view"):
        context = context or access()
        expected = [await resource_is_visible(self.conn, item, context, permission=permission) for item in rows]
        self.assertEqual(await resources_are_visible(self.conn, rows, context, permission=permission), expected)
        return expected

    async def test_visibility_tenant_owner_permission_and_version_matrix_matches_scalar(self):
        rows = []
        combinations = product(
            ("tenant", "foreign"), ("private", "shared", "public", "installation_shared", "quarantined"),
            ("user:reader", "agent:reader", "user:other", None), (1, 2),
        )
        for index, (tenant, visibility, owner, version) in enumerate(combinations):
            identifier = f"resource-{index}"
            rows.append(row(identifier, tenant_id=tenant, visibility=visibility,
                            owner_principal_id=owner, acl_version=version))
            if index % 3:
                await self.grant(identifier, tenant=tenant, permission="admin" if index % 2 else "view")
        for permission in ("view", "search", "admin", "reveal_sensitive"):
            with self.subTest(permission=permission):
                await self.assert_scalar_parity(rows, permission=permission)

    async def test_user_agent_device_and_installation_grants_remain_typed(self):
        rows = []
        for kind, principal_id in (("user", "reader"), ("agent", "reader"), ("device", "device"),
                                   ("installation", "tenant"), ("user", "agent:reader")):
            identifier = kind + ":" + principal_id
            await self.grant(identifier, principal_type=kind, principal_id=principal_id)
            rows.append(row(identifier))
        self.assertEqual(await self.assert_scalar_parity(rows, access("user")), [True, False, True, True, False])
        self.assertEqual(await self.assert_scalar_parity(rows, access("agent")), [False, True, True, True, False])

    async def test_grants_are_bound_to_exact_resource_kind_tenant_and_permission(self):
        await self.grant("resource")
        variants = [row(), row(resource_type="ui_view"), row(tenant_id="foreign")]
        self.assertEqual(await self.assert_scalar_parity(variants), [True, False, False])
        self.assertEqual(await self.assert_scalar_parity(variants, permission="admin"), [False, False, False])
        await self.grant("resource", permission="search")
        self.assertEqual(await self.assert_scalar_parity(variants, permission="search"), [True, False, False])
        await self.grant("resource", permission="admin")
        self.assertTrue((await self.assert_scalar_parity([row()], permission="reveal_sensitive"))[0])

    async def test_batch_preserves_input_order_duplicate_rows_and_sqlite_rows(self):
        await self.grant("allowed")
        result = await self.conn.execute("""SELECT 'tenant' AS tenant_id,'session' AS resource_type,
            'allowed' AS resource_id,'private' AS visibility,'user:other' AS owner_principal_id,1 AS acl_version""")
        saved = await result.fetchone()
        rows = [row("missing"), saved, row("allowed", acl_version=2), saved, row(owner_principal_id="user:reader")]
        self.assertEqual(await self.assert_scalar_parity(rows), [False, True, False, True, True])

    async def test_empty_and_fast_paths_need_neither_grant_columns_nor_acl_table(self):
        await self.conn.execute("DROP TABLE resource_acl")
        rows = [
            {"tenant_id": "foreign"},
            {"tenant_id": "tenant", "visibility": "quarantined"},
            {"tenant_id": "tenant", "visibility": "private", "owner_principal_id": "user:reader"},
            {"tenant_id": "tenant", "visibility": "public", "owner_principal_id": None},
        ]
        self.assertEqual(await resources_are_visible(self.conn, [], access()), [])
        self.assertEqual(await self.assert_scalar_parity(rows), [False, False, True, True])
        no_grants = replace(access(), grant_identities=frozenset())
        self.assertEqual(await self.assert_scalar_parity([row()], no_grants), [False])

    async def test_fresh_read_after_fetch_revoke_regrant_and_version_change(self):
        await self.grant("resource")
        await self.conn.commit()
        fetched = row()
        self.assertEqual(await resources_are_visible(self.conn, [fetched], access()), [True])
        await self.conn.execute("DELETE FROM resource_acl WHERE resource_id='resource'")
        await self.conn.commit()
        self.assertEqual(await resources_are_visible(self.conn, [fetched], access()), [False])
        await self.grant("resource", version=2)
        self.assertEqual(await resources_are_visible(self.conn, [fetched], access()), [False])
        self.assertEqual(await resources_are_visible(self.conn, [row(acl_version=2)], access()), [True])
        self.assertEqual(await resources_are_visible(self.conn, [row(acl_version=2, visibility="quarantined")], access()), [False])

    async def test_separate_writer_revokes_a_grant_after_rows_were_fetched(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "acl.sqlite3"
            async with aiosqlite.connect(path) as reader, aiosqlite.connect(path) as writer:
                reader.row_factory = aiosqlite.Row
                await reader.executescript(operational_schema_sql())
                await reader.commit()
                await writer.execute("""INSERT INTO resource_acl
                    (tenant_id,resource_type,resource_id,principal_type,principal_id,permission,acl_version,granted_at_ms)
                    VALUES ('tenant','session','resource','user','reader','view',1,1)""")
                await writer.commit()
                fetched = await (await reader.execute("""SELECT 'tenant' AS tenant_id,'session' AS resource_type,
                    'resource' AS resource_id,'private' AS visibility,'user:other' AS owner_principal_id,1 AS acl_version""")).fetchone()
                self.assertEqual(await resources_are_visible(reader, [fetched], access()), [True])
                await writer.execute("DELETE FROM resource_acl")
                await writer.commit()
                self.assertEqual(await resources_are_visible(reader, [fetched], access()), [False])
                self.assertFalse(await resource_is_visible(reader, fetched, access()))

    async def test_opaque_identifiers_are_bound_exactly_without_interpolation_or_casting(self):
        identifiers = ["a' OR 1=1 --", "opaque\x00tail", "opaque", "unicode-é-雪-🧭", "slash/%_\\quote\"", "x" * 100000]
        for identifier in identifiers[::2]:
            await self.grant(identifier)
        self.assertEqual(await self.assert_scalar_parity([row(value) for value in identifiers]), [True, False, True, False, True, False])
        await self.grant("opaque\x00tail")
        self.assertTrue((await self.assert_scalar_parity([row("opaque\x00tail")]))[0])
        self.assertFalse((await self.assert_scalar_parity([row("opaque\x00other")]))[0])

    async def test_resource_and_principal_batches_are_bounded_without_losing_matches(self):
        identities = frozenset(("role", f"role-{index}") for index in range(129))
        context = replace(access(), grant_identities=identities)
        rows = [row(f"resource-{index}") for index in range(403)]
        for index in range(403):
            await self.grant(f"resource-{index}", principal_type="role", principal_id=f"role-{index % 129}")
        sql = []
        await self.conn.set_trace_callback(sql.append)
        try:
            self.assertEqual(await resources_are_visible(self.conn, rows, context), [True] * 403)
        finally:
            await self.conn.set_trace_callback(None)
        batches = [query for query in sql if "operational-resource-visibility-batch" in query]
        self.assertEqual(len(batches), 9)  # ceil(403/200) * ceil(129/64)
        self.assertTrue(all(query.lstrip().startswith("/*") for query in batches))
        await self.assert_scalar_parity(rows, context)

    async def test_read_never_mutates_commits_or_owns_the_callers_transaction(self):
        await self.conn.execute("BEGIN")
        await self.grant("resource")
        before = self.conn.total_changes
        self.assertEqual(await resources_are_visible(self.conn, [row()], access()), [True])
        self.assertTrue(self.conn.in_transaction)
        self.assertEqual(self.conn.total_changes, before)
        await self.conn.rollback()
        self.assertEqual(await resources_are_visible(self.conn, [row()], access()), [False])

    async def test_database_failure_does_not_become_a_positive_decision(self):
        await self.conn.execute("DROP TABLE resource_acl")
        with self.assertRaises(sqlite3.OperationalError):
            await resources_are_visible(self.conn, [row()], access())

    async def test_cancellation_propagates_without_caching_or_mutating_storage(self):
        await self.grant("resource")
        entered = asyncio.Event()
        release = asyncio.Event()
        class SuspendedConnection:
            async def execute(self, *_args):
                entered.set()
                await release.wait()
                raise AssertionError("cancelled read continued")
        task = asyncio.create_task(resources_are_visible(SuspendedConnection(), [row()], access()))
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await self.conn.execute("DELETE FROM resource_acl")
        self.assertEqual(await resources_are_visible(self.conn, [row()], access()), [False])

    async def test_version_normalization_and_failures_match_scalar_binding(self):
        await self.grant("resource")
        for version in (True, "01", 1.0, 0, -1, 2):
            with self.subTest(version=version):
                await self.assert_scalar_parity([row(acl_version=version)])
        for version, failure in (("invalid", ValueError), (None, TypeError), (2 ** 80, OverflowError)):
            with self.subTest(version=version):
                with self.assertRaises(failure):
                    await resource_is_visible(self.conn, row(acl_version=version), access())
                with self.assertRaises(failure):
                    await resources_are_visible(self.conn, [row(acl_version=version)], access())

    async def test_no_implicit_trust_for_none_and_same_explicit_service_context(self):
        self.assertEqual(await resources_are_visible(self.conn, [], None), [])
        with self.assertRaises(AttributeError):
            await resource_is_visible(self.conn, row(), None)
        with self.assertRaises(AttributeError):
            await resources_are_visible(self.conn, [row()], None)
        service = AccessContext("tenant", "system:service", "system", "service", "", frozenset({"system:service"}),
                                frozenset({("system", "service")}))
        await self.grant("service-grant", principal_type="system", principal_id="service")
        rows = [row(owner_principal_id="system:service"), row("service-grant"), row("ungranted")]
        self.assertEqual(await self.assert_scalar_parity(rows, service), [True, True, False])

    async def test_host_expiry_and_custom_policy_are_still_required_before_publication(self):
        await self.grant("resource")
        principal = PrincipalRef("fixture", "tenant", "reader")
        context = ExecutionContext(principal, principal, principal, "resource", "agent", (principal,))
        resource = ResourceRef("session", "tenant", "resource")
        clock = SimpleNamespace(now=1, expires_at=2, permitted=True, epoch=1, expected_epoch=1)
        class Policy:
            async def authorize(self, *_args, **_kwargs):
                return clock.permitted and clock.now < clock.expires_at and clock.epoch == clock.expected_epoch
        policy = Policy()
        await require_authorized(policy, context, "session.read", resource)
        self.assertEqual(await resources_are_visible(self.conn, [row()], access()), [True])
        clock.now = clock.expires_at
        with self.assertRaises(PermissionError):
            await require_authorized(policy, context, "session.publish", resource)
        clock.now, clock.permitted, clock.epoch = 1, True, 2
        with self.assertRaises(PermissionError):
            await require_authorized(policy, context, "session.publish", resource)
        clock.epoch, clock.permitted = clock.expected_epoch, False
        with self.assertRaises(PermissionError):
            await require_authorized(policy, context, "session.publish", resource)
        clock.permitted = True
        await require_authorized(policy, context, "session.publish", resource)
        # A static DB grant is not a substitute for those host policy checks.
        self.assertEqual(await resources_are_visible(self.conn, [row()], access()), [True])


if __name__ == "__main__":
    unittest.main()
