"""Schema-wide provider and account slots, with database-clock cooldowns."""

import asyncio
from contextlib import asynccontextmanager
from uuid import uuid4

from openagent_core.models.credential_pool import retry_after_seconds, rate_limit_cooldown


def _rate_limited(error):
    for _ in range(8):
        if error is None:
            return False
        if getattr(error, "status_code", None) in {429, 529}:
            return True
        error = error.__cause__ or error.__context__
    return False


class ProviderSlots:
    async def _reserve_provider(self, context, run_id, route, slot_id):
        async with self._db() as db:
            run = await self._owned(db, run_id)
            if (
                run["status"] != "running"
                or run["cancel_requested"]
                or run["tenant"] != context.tenant_id
                or run["session_id"] != context.session_id
                or context.agent_id != self.agent_id
            ):
                raise PermissionError("Provider request no longer belongs to a running turn")
            await db.execute(
                "INSERT INTO provider_limits(provider,capacity) VALUES(%s,%s) ON CONFLICT DO NOTHING",
                (route.provider_id, route.capacity),
            )
            # This short lock covers every admission for one physical provider, across agents.
            row = await (
                await db.execute(
                    "SELECT capacity FROM provider_limits WHERE provider=%s FOR UPDATE",
                    (route.provider_id,),
                )
            ).fetchone()
            if row["capacity"] != route.capacity:
                raise ValueError("Inconsistent distributed provider limit")
            boundary = (route.provider_id, context.authority.authority, context.tenant_id)
            tenant_capacity = route.tenant_capacity or route.capacity
            await db.execute(
                """INSERT INTO provider_tenants(provider,authority,tenant,capacity)
                VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                (*boundary, tenant_capacity),
            )
            tenant = await (
                await db.execute(
                    """SELECT capacity FROM provider_tenants
                    WHERE provider=%s AND authority=%s AND tenant=%s""",
                    boundary,
                )
            ).fetchone()
            if tenant["capacity"] != tenant_capacity:
                raise ValueError("Inconsistent distributed tenant-provider limit")
            counts = await (
                await db.execute(
                    """
                SELECT p.account,count(*) AS active,count(*) FILTER (WHERE p.authority=%s AND p.tenant=%s) AS tenant_active FROM provider_attempts p JOIN workers w ON w.id=p.worker
                WHERE p.provider=%s AND w.expires_at>clock_timestamp() GROUP BY p.account
                """,
                    (context.authority.authority, context.tenant_id, route.provider_id),
                )
            ).fetchall()
            active = {r["account"]: r["active"] for r in counts}
            if (
                sum(active.values()) >= route.capacity
                or sum(r["tenant_active"] for r in counts) >= tenant_capacity
            ):
                return None
            for account in route.accounts:
                await db.execute(
                    """INSERT INTO provider_accounts(provider,account,capacity) VALUES (%s,%s,%s)
                    ON CONFLICT DO NOTHING""",
                    (route.provider_id, account.account_id, account.capacity),
                )
                state = await (
                    await db.execute(
                        """SELECT capacity,coalesce(cooldown_until>clock_timestamp(),false) AS cooling
                    FROM provider_accounts WHERE provider=%s AND account=%s""",
                        (route.provider_id, account.account_id),
                    )
                ).fetchone()
                if state["capacity"] != account.capacity:
                    raise ValueError("Inconsistent distributed account limit")
                if not state["cooling"] and active.get(account.account_id, 0) < account.capacity:
                    await db.execute(
                        """INSERT INTO provider_attempts(id,provider,account,worker,agent,run_id,authority,tenant)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (
                            slot_id,
                            route.provider_id,
                            account.account_id,
                            self._worker,
                            self.agent_id,
                            run_id,
                            context.authority.authority,
                            context.tenant_id,
                        ),
                    )
                    return account
            return None

    async def check_provider_slot(self, context, run_id, provider, account):
        async with self._db() as db:
            row = await self._owned(db, run_id)
            if (
                row["cancel_requested"]
                or row["status"] != "running"
                or (row["tenant"], row["session_id"]) != (context.tenant_id, context.session_id)
            ):
                raise PermissionError("Provider execution is no longer authorized")
            live = await (
                await db.execute(
                    """SELECT 1 FROM provider_attempts WHERE worker=%s AND agent=%s
                AND run_id=%s AND provider=%s AND account=%s""",
                    (self._worker, self.agent_id, run_id, provider, account),
                )
            ).fetchone()
            if not live:
                raise PermissionError("Provider permit expired")

    async def _watch_provider_owner(self, task):
        loop = asyncio.get_running_loop()
        while True:
            remaining = self._heartbeat_ack + self.lease_seconds - loop.time()
            if remaining <= 0:
                task.cancel()
                return
            await asyncio.sleep(remaining)

    @asynccontextmanager
    async def provider_slot(self, context, run_id, route):
        from openagent_core.provider_admission import ProviderRoute

        if not isinstance(route, ProviderRoute):
            raise ValueError("Expected an authorized provider route")
        slot_id = uuid4()
        account = None
        watchdog = None
        try:
            async with asyncio.timeout(route.wait_seconds):
                while account is None:
                    account = await self._reserve_provider(context, run_id, route, slot_id)
                    if account is None:
                        await asyncio.sleep(0.1)
            watchdog = asyncio.create_task(self._watch_provider_owner(asyncio.current_task()))
            try:
                yield account
            except Exception as error:
                if _rate_limited(error):
                    delay = rate_limit_cooldown(retry_after_seconds(error))
                    async with self._db() as db:
                        await self._fence(db)
                        await db.execute(
                            """UPDATE provider_accounts SET cooldown_until=greatest(
                            cooldown_until,clock_timestamp()+make_interval(secs=>%s))
                            WHERE provider=%s AND account=%s""",
                            (delay, route.provider_id, account.account_id),
                        )
                raise
        finally:
            if watchdog is not None:
                watchdog.cancel()
                await asyncio.gather(watchdog, return_exceptions=True)
            # Include cancellation at the commit boundary before the coroutine receives a grant.
            # Worker expiry also removes abandoned slots after a process crash.
            async with self._db() as db:
                await db.execute(
                    "DELETE FROM provider_attempts WHERE id=%s AND worker=%s",
                    (slot_id, self._worker),
                )
