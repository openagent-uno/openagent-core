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
        await self.scheduler.start()
        self.running=True

    async def close(self):
        self.running=False
        await self.scheduler.stop()

    async def ready(self):return self.running

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
