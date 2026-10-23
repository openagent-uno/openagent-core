"""Terminal tool reads settle notifications without changing their transcript."""
import asyncio
import unittest
from types import SimpleNamespace

from openagent_core.jobs import BackgroundJobs, JobEvent


def completion(job="job"):
    return JobEvent(job, "completed", "source", "output", {"job_id": job}, "Finished")


class ObservedCompletion(unittest.IsolatedAsyncioTestCase):
    async def test_observation_before_or_after_notification_suppresses_only_that_job(self):
        for notification_first in (False, True):
            with self.subTest(notification_first=notification_first):
                jobs = BackgroundJobs()
                jobs.register("job", "session", "actor-epoch")
                if notification_first:
                    jobs.complete("session", "actor-epoch", completion())
                self.assertTrue(jobs.observe_terminal("job", "session", "actor-epoch"))
                jobs.complete("session", "actor-epoch", completion())
                self.assertFalse(jobs.observe_terminal("job", "session", "actor-epoch"))
                self.assertFalse(jobs.has_running("session", context_key="actor-epoch"))
                self.assertEqual([], jobs.drain("session", context_key="actor-epoch"))

    async def test_wrong_session_or_principal_cannot_consume_notification(self):
        jobs = BackgroundJobs()
        jobs.register("job", "session", "actor-epoch")
        jobs.complete("session", "actor-epoch", completion())
        for session, context in (("other", "actor-epoch"), ("session", "revoked-epoch")):
            self.assertFalse(jobs.observe_terminal("job", session, context))
        self.assertEqual([completion().job_id], [e.job_id for e in jobs.drain("session", context_key="actor-epoch")])

    async def test_observation_preserves_other_jobs_and_wakes_terminal_waiter(self):
        jobs = BackgroundJobs()
        for job in ("observed", "other"):
            jobs.register(job, "session", "actor-epoch")
            jobs.complete("session", "actor-epoch", completion(job))
        self.assertTrue(jobs.observe_terminal("observed", "session", "actor-epoch"))
        self.assertEqual(["other"], [e.job_id for e in jobs.drain("session", context_key="actor-epoch")])
        jobs.register("joining", "session", "actor-epoch")
        waiting = asyncio.create_task(jobs.wait("session", 30, context_key="actor-epoch"))
        await asyncio.sleep(0)
        self.assertTrue(jobs.observe_terminal("joining", "session", "actor-epoch"))
        self.assertEqual([], await asyncio.wait_for(waiting, 1))

    async def test_purged_or_unknown_jobs_cannot_be_revived_by_late_completion(self):
        jobs = BackgroundJobs()
        jobs.register("job", "session", "actor-epoch")
        await jobs.purge_session("session")
        self.assertFalse(jobs.observe_terminal("job", "session", "actor-epoch"))
        jobs.complete("session", "actor-epoch", completion())
        self.assertEqual([], jobs.drain("session", context_key="actor-epoch"))

    async def test_observing_one_job_does_not_stop_waiting_for_another_running_job(self):
        jobs = BackgroundJobs()
        for job in ("joined", "other"):
            jobs.register(job, "session", "actor-epoch")
        waiting = asyncio.create_task(jobs.wait("session", 30, context_key="actor-epoch"))
        await asyncio.sleep(0)
        jobs.observe_terminal("joined", "session", "actor-epoch")
        await asyncio.sleep(0)
        self.assertFalse(waiting.done())
        jobs.complete("session", "actor-epoch", completion("other"))
        self.assertEqual(["other"], [e.job_id for e in await asyncio.wait_for(waiting, 1)])

    async def test_executor_observer_runs_only_after_authorized_result_publication(self):
        from openagent_core import ExecutionContext, PrincipalRef
        from openagent_core.capabilities import CapabilityCatalog, CapabilityUnavailable, FunctionSource, ToolDefinition
        principal = PrincipalRef("host", "tenant", "alice")
        context = ExecutionContext(principal, principal, principal, "session", "agent", (principal,))
        for revoked, source_removed in ((False, False), (True, False), (False, True)):
            with self.subTest(revoked=revoked, source_removed=source_removed):
                allowed, observed, order = True, [], []
                async def authorize(_context, action, *args, **kwargs):
                    return allowed or action != "tool.publish"
                catalog = CapabilityCatalog(SimpleNamespace(authorize=authorize))
                async def read(_args, _context):
                    nonlocal allowed
                    allowed = not revoked
                    if source_removed:
                        catalog.revoke("source")
                    return {"still_running": False}
                class Source(FunctionSource):
                    async def observe_published_result(_self, name, arguments, result, current):
                        observed.append((name, arguments, result, current))
                        order.append("executor")
                async def normal_observer(*args): order.append("normal")
                catalog.observers = (normal_observer,)
                source = Source((ToolDefinition("output", "Read result", {}),), {"output": read})
                catalog.register("source", source, source, target_label="fixture")
                ref = (await catalog.discover(context))[0].tool_ref
                if revoked or source_removed:
                    with self.assertRaises((PermissionError, CapabilityUnavailable)):
                        await catalog.call_tool(ref, {"job_id": "job"}, context)
                    self.assertEqual([], observed)
                else:
                    result = await catalog.call_tool(ref, {"job_id": "job"}, context)
                    self.assertEqual({"still_running": False}, result)
                    self.assertEqual(["normal", "executor"], order)
                    self.assertEqual(result, observed[0][2])


class ActualAutoloop(unittest.IsolatedAsyncioTestCase):
    async def execute(self, *, observed):
        from openagent_core.core.agent import Agent
        from openagent_core.engine import AgentExecutor
        hub, inputs, events = BackgroundJobs(), [], []
        class Model:
            model = "fixture"; history_mode = "caller"
            def effective_model_id(self, _sid=None): return self.model
            async def close_session(self, _sid): pass
            async def stream(self, messages, system=None, session_id=None, on_status=None, on_final_answer=None):
                inputs.append(messages)
                if len(inputs) == 1:
                    hub.register("clone", "session", "unbound")
                    hub.complete("session", "unbound", completion("clone"))
                    if observed:
                        hub.observe_terminal("clone", "session", "unbound")
                    text = "Prima conclusione italiana."
                else:
                    text = "Conclusione italiana completa e autonoma."
                await on_final_answer({"text": text})
                yield text
        agent = Agent(name="fixture", model=Model(), memory=None, system_prompt="fixture", background_jobs=hub)
        self.addAsyncCleanup(agent.shutdown)
        async def allow(*args, **kwargs): return True
        async def append(*args): events.append(args[-1])
        runtime = SimpleNamespace(services=SimpleNamespace(authorizer=SimpleNamespace(authorize=allow),
            store=SimpleNamespace(append_event=append)))
        context = SimpleNamespace(tenant_id="tenant", session_id="session", agent_id="agent", audience=(),
            author=SimpleNamespace(kind="user", key="alice"))
        request = SimpleNamespace(input="Rispondi in italiano alla richiesta originale.", session_id="session",
            run_id="run", model_ref=None, attachments=())
        await AgentExecutor(agent).execute(request, context, runtime)
        return inputs, events

    async def test_explicit_terminal_read_does_not_trigger_redundant_model_iteration(self):
        inputs, events = await self.execute(observed=True)
        self.assertEqual(1, len(inputs))
        self.assertFalse(any(e.get("kind") == "iteration_break" for e in events))
        self.assertEqual("Prima conclusione italiana.", events[-1]["final_answer"]["text"])

    async def test_unobserved_completion_keeps_two_iterations_with_complete_final_contract(self):
        import json
        inputs, events = await self.execute(observed=False)
        self.assertEqual(2, len(inputs))
        reminder = json.dumps(inputs[-1], ensure_ascii=False)
        for phrase in ("not a new human message", "may not have been delivered", "self-contained final answer",
                       "original user's language", "tool confirmed actual delivery"):
            self.assertIn(phrase, reminder)
        self.assertEqual("Prima conclusione italiana.Conclusione italiana completa e autonoma.", events[-1]["text"])
        self.assertEqual("Conclusione italiana completa e autonoma.", events[-1]["final_answer"]["text"])
        self.assertEqual(1, sum(e.get("kind") == "iteration_break" for e in events))
