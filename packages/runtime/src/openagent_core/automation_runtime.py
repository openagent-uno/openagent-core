"""Explicit lifecycle for the shared scheduler algorithms.

The host provides execution_service; it owns definition authorization and
submits every firing to Runtime. This facade never manufactures a principal,
delegation or default automation.
"""
import asyncio
from .core.scheduler import Scheduler


class _DomainWorker:
    def __init__(self, owner, domain):
        self.owner, self.domain = owner, domain

    async def start(self, runtime):
        await self.owner.enable(self.domain, runtime)

    async def close(self):
        await self.owner.disable(self.domain)

class AutomationRuntime:
    def __init__(self,db,agent,execution_service,*,broadcast=None,
                 enabled_domains=("scheduler", "workflows", "events")):
        if execution_service is None:raise ValueError("An explicit automation execution service is required")
        self.scheduler=Scheduler(db,agent,broadcast=broadcast,execution_service=execution_service,
                                 enabled_domains=())
        self.default_domains=frozenset(enabled_domains)
        if self.default_domains - {"scheduler", "workflows", "events"}:
            raise ValueError("Unknown automation domain")
        self._domains=set()
        self._workers={domain:_DomainWorker(self,domain) for domain in self.default_domains}
        self._lock=asyncio.Lock()
        self.running=False

    def worker(self, domain):
        if domain not in {"scheduler", "workflows", "events"}:
            raise ValueError("Unknown automation domain")
        return self._workers.setdefault(domain, _DomainWorker(self, domain))

    async def enable(self, domain, runtime):
        async with self._lock:
            self.runtime=runtime
            previous=set(self._domains)
            self._domains.add(domain)
            try:
                await self.scheduler.set_enabled_domains(self._domains)
                if not self.running:
                    await self.scheduler.start()
                    self.running=True
            except Exception:
                self._domains=previous
                await self.scheduler.set_enabled_domains(previous)
                raise

    async def disable(self, domain):
        async with self._lock:
            self._domains.discard(domain)
            await self.scheduler.set_enabled_domains(self._domains)
            if self.running and not self._domains:
                self.running=False
                await self.scheduler.stop()

    async def start(self,runtime):
        for domain in self.default_domains:
            await self.enable(domain,runtime)

    async def close(self):
        async with self._lock:
            self._domains.clear()
            await self.scheduler.set_enabled_domains(())
            if self.running:
                self.running=False
                await self.scheduler.stop()

    async def ready(self):return self.running

    def launch(self,kind,row,*,request_id,inputs=None):
        """Track a host-authorized manual firing independent of HTTP lifetime."""
        if not self.running:raise RuntimeError("Automation runtime is not started")
        from .runtime import runtime_scope
        async def execute():
            with runtime_scope(self.runtime):
                if kind=="scheduled_task":
                    if "scheduler" not in self._domains:raise RuntimeError("The scheduler module is not active")
                    return await self.scheduler.run_task(row,trigger="manual",request_id=request_id)
                if kind=="workflow":
                    if "workflows" not in self._domains:raise RuntimeError("The workflows module is not active")
                    return await self.scheduler.run_workflow(row,trigger="manual",request_id=request_id,inputs=inputs)
                raise ValueError("Unknown automation kind")
        return self.scheduler._spawn_workflow(execute())

    async def cancel_runs(self,kind,definition_id,run_ids):
        """Flag only the exact, host-authorized projected run IDs atomically."""
        from .automation import AutomationRepository
        table,column={"workflow":("workflow_runs","workflow_id"),"scheduled_task":("task_runs","task_id")}[kind]
        repository=AutomationRepository(self.scheduler.db.db_path)
        flagged=[]
        async with repository.transaction() as connection:
            for run_id in run_ids:
                cursor=await connection.execute(f"UPDATE {table} SET status='cancelling' WHERE id=? AND {column}=? AND status='running'",(run_id,definition_id))
                if cursor.rowcount:flagged.append(run_id)
        return flagged

    async def drain(self):
        """Run one deterministic due/queue pass, also useful for host tests."""
        if not self.running:raise RuntimeError("Automation runtime is not started")
        await self.scheduler._check_and_run()
        if "scheduler" in self._domains:await self.scheduler._drain_task_run_requests()
        if "events" in self._domains:await self.scheduler._drain_event_deliveries()

    async def settle(self):
        """Wait for currently tracked dispatches without accepting a new one."""
        import asyncio
        await asyncio.gather(*tuple(self.scheduler._workflow_tasks))
