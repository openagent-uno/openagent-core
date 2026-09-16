"""Explicit lifecycle for the shared scheduler algorithms.

The host provides execution_service; it owns definition authorization and
submits every firing to Runtime. This facade never manufactures a principal,
delegation or default automation.
"""
from .core.scheduler import Scheduler

class AutomationRuntime:
    def __init__(self,db,agent,execution_service,*,broadcast=None):
        if execution_service is None:raise ValueError("An explicit automation execution service is required")
        self.scheduler=Scheduler(db,agent,broadcast=broadcast,execution_service=execution_service)
        self.running=False

    async def start(self,runtime):
        self.runtime=runtime
        await self.scheduler.start()
        self.running=True

    async def close(self):
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
                    return await self.scheduler.run_task(row,trigger="manual",request_id=request_id)
                if kind=="workflow":
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
        await self.scheduler._drain_task_run_requests()
        await self.scheduler._drain_event_deliveries()

    async def settle(self):
        """Wait for currently tracked dispatches without accepting a new one."""
        import asyncio
        await asyncio.gather(*tuple(self.scheduler._workflow_tasks))
