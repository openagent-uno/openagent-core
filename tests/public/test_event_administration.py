from pathlib import Path
import tempfile
import unittest
from openagent_core.engine import MemoryDB
from openagent_core.event_administration import EventAdministration
from openagent_core.administration import ManagementContext
from openagent_core import PrincipalRef

class Policy:
    def __init__(self):self.allowed=True
    async def authorize(self,context,action,resource,*,audience=()):return self.allowed

class EventTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.db=MemoryDB(str(Path(self.tmp.name)/"state.sqlite"));await self.db.connect()
        self.policy=Policy();self.captures=[];self.capture_denied=False
        async def capture(kind,row,context,connection):
            if self.capture_denied:raise PermissionError("revoked")
            self.captures.append((kind,row["id"],row["enabled"],row["prompt_template"]))
        async def ready():return True
        self.service=EventAdministration(self.db,self.policy,capture=capture,queue_ready=ready)
        self.context=ManagementContext(PrincipalRef("host","tenant","alice","user"),"agent")
    async def asyncTearDown(self):await self.db.close();self.tmp.cleanup()

    async def create(self):return await self.service.write(self.context,{"name":"Release","action_kind":"prompt","prompt_template":"Inspect {{ payload.value }}"})

    async def test_secret_once_preserve_identity_and_capture_failure_rolls_back(self):
        row=await self.create();identifier=row["id"];secret=row["secret"]
        self.assertNotIn("secret",await self.service.get(self.context,identifier))
        self.assertNotIn("secret_enc",row)
        before=await self.db.get_event(identifier,include_secret=True)
        self.assertNotIn(secret,str(before))
        self.capture_denied=True
        with self.assertRaises(PermissionError):await self.service.write(self.context,{"prompt_template":"Changed","enabled":False},identifier)
        self.assertEqual(before,await self.db.get_event(identifier,include_secret=True))
        self.capture_denied=False
        updated=await self.service.write(self.context,{"description":"Description"},identifier)
        self.assertEqual(identifier,updated["id"]);self.assertEqual(before["slug"],updated["slug"])
        rotated=await self.service.rotate(self.context,identifier)
        self.assertNotEqual(secret,rotated["secret"])
        self.assertEqual(before["prompt_template"],rotated["prompt_template"])
        self.assertEqual(0,len(await self.db.list_event_deliveries(identifier)))

    async def test_enqueue_is_durable_unclaimed_and_revocation_precedes_delivery(self):
        row=await self.create()
        result=await self.service.trigger(self.context,row["id"],payload={"value":"test"},wait=False)
        delivery=await self.db.get_event_delivery(result["delivery_id"])
        self.assertEqual("received",delivery["status"]);self.assertIsNone(delivery["claimed_at"])
        self.assertIn('"value": "test"',delivery["payload_json"])
        self.policy.allowed=False
        with self.assertRaises(PermissionError):await self.service.trigger(self.context,row["id"],wait=False)
        self.assertEqual(1,len(await self.db.list_event_deliveries(row["id"])))

    async def test_borrowed_delivery_transaction_rolls_back_enqueue_and_timestamp(self):
        event=await self.create()
        with self.assertRaises(RuntimeError):
            async with self.service.repository.transaction() as connection:
                db=self.service.repository.definition_store(connection)
                await db.add_event_delivery(event_id=event["id"],payload={"rollback":True},claimed=False)
                raise RuntimeError("Abort host transaction")
        self.assertEqual([],await self.db.list_event_deliveries(event["id"]))
        self.assertIsNone((await self.db.get_event(event["id"]))["last_triggered_at"])

    async def test_invalid_binding_limits_and_unknown_fields_never_write(self):
        for fields in ({"authority":"forged"},{"rate_limit_per_min":0},{"session_binding_enabled":True},{"enabled":"false"}):
            with self.assertRaises(ValueError):await self.service.write(self.context,{"name":"Invalid","action_kind":"prompt","prompt_template":"Rules",**fields})
        self.assertEqual([],await self.db.list_events())
