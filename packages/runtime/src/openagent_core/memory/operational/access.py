"""Canonical principal and ACL checks for operational history/search.

Callers never supply a principal.  The gateway builds :class:`AccessContext`
from the authenticated device certificate and every API/search candidate is
rechecked against the canonical operational database before serialization.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Sequence

from ..legacy_document_history import SessionProjectionGuard


@dataclass(frozen=True)
class AccessContext:
    tenant_id: str
    principal_id: str
    principal_type: str
    handle: str
    device_id: str
    principal_ids: frozenset[str]
    grant_identities: frozenset[tuple[str, str]]

    projection: SessionProjectionGuard | None = field(default=None, compare=False, repr=False)

    @classmethod
    def from_request(cls, request: Any) -> "AccessContext":
        cert = request.get("device_cert")
        tenant = str(request.get("network_id") or getattr(cert, "network_id", "")).strip()
        handle = str(request.get("user_handle") or getattr(cert, "handle", "")).strip()
        device = str(request.get("client_id") or getattr(cert, "device_pubkey_hex", "")).strip()
        if cert is None or not tenant or not handle or not device:
            raise PermissionError("authenticated device context is required")
        capabilities = set(getattr(cert, "capabilities", ()) or ())
        principal_type = "agent" if "agent" in capabilities else "user"
        principal_id = f"{principal_type}:{handle}"
        # Compatibility aliases retain their principal type. Coordinator
        # storage permits a human and an agent to have the same handle, so an
        # ambiguous raw handle can never be an authorization identity.
        typed_handle = f"{principal_type}:{handle}"
        typed_device = f"{principal_type}:{device}"
        ids = frozenset(
            {
                typed_handle,
                typed_device,
                f"device:{device}",
            }
        )
        grants = frozenset(
            {
                (principal_type, handle),
                (principal_type, typed_handle),
                ("device", device),
                ("device", f"device:{device}"),
                ("installation", tenant),
            }
        )
        return cls(tenant, principal_id, principal_type, handle, device, ids, grants,
                   request.get("openagent_session_projection") or SessionProjectionGuard())

    @classmethod
    def from_on_behalf_identity(cls, identity: Any) -> "AccessContext":
        """Build the same ACL identity from a server-bound turn context.

        The operational-search MCP deliberately exposes no identity fields in
        its tool schema.  Its in-process adapter reads an
        :class:`~openagent_core.core.on_behalf_context.OnBehalfIdentity` that the gateway
        copied from the verified certificate and converts it here, preserving
        exact parity with REST authorization.
        """

        if identity is None:
            raise PermissionError("authenticated on-behalf-of context is required")
        tenant = str(getattr(identity, "tenant_id", "") or "").strip()
        handle = str(getattr(identity, "handle", "") or "").strip()
        device = str(getattr(identity, "device_id", "") or "").strip()
        principal_type = str(getattr(identity, "principal_type", "") or "").strip()
        if principal_type not in {"user", "agent"} or not tenant or not handle or not device:
            raise PermissionError("authenticated on-behalf-of context is incomplete")
        principal_id = f"{principal_type}:{handle}"
        ids = frozenset(
            {
                principal_id,
                f"{principal_type}:{device}",
                f"device:{device}",
            }
        )
        grants = frozenset(
            {
                (principal_type, handle),
                (principal_type, principal_id),
                ("device", device),
                ("device", f"device:{device}"),
                ("installation", tenant),
            }
        )
        return cls(
            tenant,
            principal_id,
            principal_type,
            handle,
            device,
            ids,
            grants,
        )


def row_is_visible_without_grant(row: Any, access: AccessContext) -> bool:
    if str(row["tenant_id"]) != access.tenant_id:
        return False
    visibility = str(row["visibility"])
    if visibility == "quarantined":
        return False
    if visibility in {"installation_shared", "public"}:
        return True
    owner = row["owner_principal_id"]
    return owner is not None and str(owner) in access.principal_ids


async def resource_is_visible(conn, row, access, *, permission="view") -> bool:
    if not await _resource_acl_visible(conn, row, access, permission=permission):
        return False
    if "resource_type" in row.keys() and row["resource_type"] == "session":
        projection = getattr(access, "projection", None)
        host_only = permission not in {"view", "search"} or projection is None
        guard = projection or SessionProjectionGuard()
        return bool(await guard.visible(conn, (row["resource_id"],), host_only=host_only))
    return True


async def _resource_acl_visible(
    conn: Any,
    row: Any,
    access: AccessContext,
    *,
    permission: str = "view",
) -> bool:
    """Recheck one canonical resource, including explicit ACL grants.

    Installation/public visibility grants read/search access only.  Mutating
    permissions such as ``admin`` still require ownership or an explicit ACL;
    otherwise a readable shared dashboard would also be writable/executable by
    every principal in the tenant.
    """

    if str(row["tenant_id"]) != access.tenant_id or str(row["visibility"]) == "quarantined":
        return False
    owner = row["owner_principal_id"]
    if owner is not None and str(owner) in access.principal_ids:
        return True
    if (
        permission in {"view", "search"}
        and str(row["visibility"]) in {"installation_shared", "public"}
    ):
        return True
    for principal_type, principal_id in access.grant_identities:
        match = await (
            await conn.execute(
                "SELECT 1 FROM resource_acl WHERE tenant_id=? AND resource_type=? "
                "AND resource_id=? AND principal_type=? AND principal_id=? "
                "AND permission IN (?, 'admin') AND acl_version=? LIMIT 1",
                (
                    access.tenant_id,
                    str(row["resource_type"]),
                    str(row["resource_id"]),
                    principal_type,
                    principal_id,
                    permission,
                    int(row["acl_version"]),
                ),
            )
        ).fetchone()
        if match is not None:
            return True
    return False


# 200 resources * 4 fields + 64 identities * 2 fields + tenant/permission
# fits SQLite's conservative 999-variable limit. Bound values preserve opaque
# identifiers exactly; no JSON decoding or SQL interpolation of IDs is used.
_VISIBILITY_RESOURCE_BATCH = 200
_VISIBILITY_IDENTITY_BATCH = 64


async def resources_are_visible(
    conn: Any,
    rows: Sequence[Any],
    access: AccessContext,
    *,
    permission: str = "view",
) -> list[bool]:
    """Fresh operational-ACL checks in input order, including duplicate rows.

    This is the batch equivalent of :func:`resource_is_visible`: the same
    tenant, quarantine, ownership, public-read, typed-grant and ACL-version
    rules apply. Explicit grants are read again on every call, in bounded
    SQLite queries. No result, principal or authorization decision is cached.

    Call after fetching candidates and again before publishing an awaited
    page. Each batch observes the caller connection's current transaction;
    this function starts/commits no transaction and never mutates storage.

    This checks only the canonical operational ACL. It does not replace a
    host/Runtime authorizer, device liveness, external-grant expiry, resource
    lifecycle or cursor validation. Those checks remain the host's boundary
    before reading and before publication.
    """
    decisions = [False] * len(rows)
    identities: tuple[tuple[str, str], ...] | None = None
    pending: list[tuple[int, str, str, int]] = []

    async def check_pending() -> None:
        if not pending:
            return
        requested_sql = ",".join("(?,?,?,?)" for _ in pending)
        requested_params = tuple(value for row in pending for value in row)
        assert identities is not None
        for start in range(0, len(identities), _VISIBILITY_IDENTITY_BATCH):
            selected_identities = identities[start:start + _VISIBILITY_IDENTITY_BATCH]
            identity_sql = ",".join("(?,?)" for _ in selected_identities)
            matches = await (await conn.execute(
                "/* operational-resource-visibility-batch */ "
                f"WITH requested(position,resource_type,resource_id,acl_version) AS (VALUES {requested_sql}), "
                f"principals(principal_type,principal_id) AS (VALUES {identity_sql}) "
                "SELECT q.position FROM requested q WHERE EXISTS ("
                "SELECT 1 FROM resource_acl acl JOIN principals p "
                "ON p.principal_type=acl.principal_type AND p.principal_id=acl.principal_id "
                "WHERE acl.tenant_id=? AND acl.resource_type=q.resource_type "
                "AND acl.resource_id=q.resource_id AND acl.acl_version=q.acl_version "
                "AND acl.permission IN (?, 'admin'))",
                (*requested_params, *(value for identity in selected_identities for value in identity),
                 access.tenant_id, permission),
            )).fetchall()
            for match in matches:
                decisions[int(match[0])] = True
        pending.clear()

    for index, row in enumerate(rows):
        # Match the scalar helper's short-circuit order. Fast-path rows need
        # neither grant columns nor an installed resource_acl table.
        if str(row["tenant_id"]) != access.tenant_id or str(row["visibility"]) == "quarantined":
            continue
        owner = row["owner_principal_id"]
        if owner is not None and str(owner) in access.principal_ids:
            decisions[index] = True
            continue
        if permission in {"view", "search"} and str(row["visibility"]) in {"installation_shared", "public"}:
            decisions[index] = True
            continue
        if identities is None:
            identities = tuple(access.grant_identities)
        if not identities:
            continue
        pending.append((index, str(row["resource_type"]), str(row["resource_id"]), int(row["acl_version"])))
        if len(pending) == _VISIBILITY_RESOURCE_BATCH:
            await check_pending()
    await check_pending()
    projection = getattr(access, "projection", None)
    sessions = {i: row["resource_id"] for i, row in enumerate(rows) if decisions[i]
                and "resource_type" in row.keys() and row["resource_type"] == "session"}
    if sessions:
        guard = projection or SessionProjectionGuard()
        allowed = await guard.visible(conn, sessions.values(), host_only=permission not in {"view", "search"} or projection is None)
        for index, sid in sessions.items():
            decisions[index] = sid in allowed
    return decisions
