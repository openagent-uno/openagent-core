"""Authorized event definitions and durable enqueue, independent of HTTP.

Hosts supply definition capture in the enclosing SQLite transaction. Capturing
may contact a parent authorization service; failure rolls back the local edit.
The scheduler remains the only delivery executor.
"""
from __future__ import annotations
import asyncio
from .automation import AutomationRepository
from .contracts import ResourceRef,require_authorized
from .core.event_secret import make_secret_material,slugify,random_slug_suffix
from .core.event_types import is_valid_type,public_types,DEFAULT_TYPE
from .core.execution_policy import normalize_execution_policy,encode_execution_policy

_FIELDS=frozenset({"name","description","type","slug","enabled","input_schema","action_kind","action_ref","prompt_template","model","session_binding_enabled","session_binding_path","execution_policy","rate_limit_per_min","max_payload_bytes"})

class EventAdministration:
    def __init__(self,db,authorizer,*,capture,public_url=None,queue_ready=None):
        self.db,self.authorizer,self.capture=db,authorizer,capture
        self.repository=AutomationRepository(db.db_path)
        self.public_url=(public_url or "").rstrip("/") or None
        self.queue_ready=queue_ready

    async def authorize(self,context,action,identifier=None):
        await require_authorized(self.authorizer,context,action,ResourceRef("event" if identifier else "agent",context.tenant_id,identifier or context.agent_id))

    def serialize(self,event):
        out={k:v for k,v in event.items() if k!="secret_enc"}
        out["webhook_path"]="/hooks/"+event["slug"]
        out["webhook_url"]=self.public_url+out["webhook_path"] if self.public_url else None
        return out

    async def types(self,context):
        await self.authorize(context,"event.read")
        return {"types":public_types()}

    async def list(self,context,*,enabled_only=False):
        await self.authorize(context,"event.read")
        rows=await self.db.list_events(enabled_only=enabled_only)
        await self.authorize(context,"event.read")
        visible=[]
        for row in rows:
            try:await self.authorize(context,"event.read",row["id"])
            except PermissionError:continue
            visible.append(self.serialize(row))
        return {"events":visible}

    async def get(self,context,identifier):
        await self.authorize(context,"event.read",identifier)
        row=await self.db.get_event(identifier)
        if row is None:raise LookupError("Event not found")
        await self.authorize(context,"event.read",identifier)
        return self.serialize(row)

    @staticmethod
    async def validated(db,fields,existing=None):
        if not isinstance(fields,dict) or set(fields)-_FIELDS:raise ValueError("Invalid event fields")
        merged={"type":DEFAULT_TYPE,"enabled":True,"input_schema":[],"rate_limit_per_min":60,"max_payload_bytes":262144,"session_binding_enabled":False,**(existing or {}),**fields}
        if not isinstance(merged.get("name"),str) or not merged["name"].strip():raise ValueError("Event name is required")
        if not is_valid_type(merged["type"]):raise ValueError("Unknown event type")
        for key in ("enabled","session_binding_enabled"):
            if not isinstance(merged[key],bool):raise ValueError("Invalid event boolean")
        for key in ("description","slug","action_ref","prompt_template","model","session_binding_path"):
            if merged.get(key) is not None and not isinstance(merged[key],str):raise ValueError("Invalid event text")
        if not isinstance(merged["input_schema"],list):raise ValueError("Invalid event input schema")
        for key,maximum in (("rate_limit_per_min",600),("max_payload_bytes",512*1024)):
            if not isinstance(merged[key],int) or isinstance(merged[key],bool) or not 1<=merged[key]<=maximum:raise ValueError("Invalid event limit")
        if merged["session_binding_enabled"] and not (merged.get("session_binding_path") or "").strip():raise ValueError("Session binding path is required")
        kind=merged.get("action_kind")
        if kind=="prompt":
            if not (merged.get("prompt_template") or "").strip():raise ValueError("Prompt template is required")
        elif kind in {"workflow","scheduled_task"}:
            ref=merged.get("action_ref")
            if not ref or await (db.get_workflow(ref) if kind=="workflow" else db.get_task(ref)) is None:raise ValueError("Referenced automation is missing")
        else:raise ValueError("Invalid event action")
        merged["execution_policy"]=normalize_execution_policy(merged.get("execution_policy"))
        return merged

    async def write(self,context,fields,identifier=None):
        await self.authorize(context,"event.write",identifier)
        async with self.repository.transaction() as connection:
            await self.authorize(context,"event.write",identifier)
            db=self.repository.definition_store(connection)
            existing=await db.get_event(identifier) if identifier else None
            if identifier and existing is None:raise LookupError("Event not found")
            if identifier and not fields:raise ValueError("No fields to update")
            merged=await self.validated(db,fields,existing)
            clear=None
            if identifier:
                updates=dict(fields)
                if "slug" in updates and updates["slug"]!=existing["slug"]:raise ValueError("Webhook slug is immutable")
                if "execution_policy" in updates:updates["execution_policy_json"]=encode_execution_policy(updates.pop("execution_policy"))
                await db.update_event(identifier,**updates)
            else:
                slug=slugify(merged.get("slug") or merged["name"])
                while await db.slug_exists(slug):slug=slugify(merged["name"])+"-"+random_slug_suffix()
                clear,encrypted,hint=make_secret_material(db_path=self.db.db_path)
                keys=("name","description","enabled","input_schema","action_kind","action_ref","prompt_template","model","session_binding_enabled","session_binding_path","execution_policy","rate_limit_per_min","max_payload_bytes")
                identifier=await db.add_event(**{k:merged[k] for k in keys if k in merged},event_type=merged["type"],slug=slug,secret_enc=encrypted,secret_hint=hint)
            row=await db.get_event(identifier)
            await self.capture("event",row,context,connection)
        result=self.serialize(row)
        if clear is not None:result["secret"]=clear
        return result

    async def delete(self,context,identifier):
        await self.authorize(context,"event.write",identifier)
        async with self.repository.transaction() as connection:
            await self.authorize(context,"event.write",identifier)
            db=self.repository.definition_store(connection)
            if await db.get_event(identifier) is None:raise LookupError("Event not found")
            await db.delete_event(identifier)
        return {"ok":True,"id":identifier}

    async def rotate(self,context,identifier):
        await self.authorize(context,"event.write",identifier)
        async with self.repository.transaction() as connection:
            await self.authorize(context,"event.write",identifier)
            db=self.repository.definition_store(connection)
            if await db.get_event(identifier) is None:raise LookupError("Event not found")
            clear,encrypted,hint=make_secret_material(db_path=self.db.db_path)
            await db.rotate_event_secret(identifier,secret_enc=encrypted,secret_hint=hint)
            row=await db.get_event(identifier)
        return {**self.serialize(row),"secret":clear}

    async def trigger(self,context,identifier,*,payload=None,wait=True,timeout_s=120,source="manual"):
        await self.authorize(context,"event.trigger",identifier)
        if not isinstance(payload or {},dict) or source not in {"manual","peer","agent"}:raise ValueError("Invalid event payload/source")
        if not isinstance(wait,bool) or not isinstance(timeout_s,int) or not 1<=timeout_s<=600:raise ValueError("Invalid event wait")
        if self.queue_ready is None or not await self.queue_ready():raise RuntimeError("Event executor is unavailable")
        async with self.repository.transaction() as connection:
            await self.authorize(context,"event.trigger",identifier)
            db=self.repository.definition_store(connection)
            row=await db.get_event(identifier)
            if row is None:raise LookupError("Event not found")
            if not row["enabled"]:raise ValueError("Event is disabled")
            # Reattest the current definition for the authenticated manual
            # initiator before inserting into the existing durable queue.
            await self.capture("event",row,context,connection)
            delivery=await db.add_event_delivery(event_id=identifier,source=source,payload=payload or {},claimed=False)
        if wait:
            deadline=asyncio.get_running_loop().time()+timeout_s
            while asyncio.get_running_loop().time()<deadline:
                row=await self.delivery(context,delivery)
                if row.get("status") not in {"received","running"}:return row
                await asyncio.sleep(.1)
        return {"delivery_id":delivery,"status":"running"}

    async def visible_delivery(self,context,row):
        await self.authorize(context,"event.read",row["event_id"])
        resource=ResourceRef("event_delivery",context.tenant_id,row["id"])
        if await self.authorizer.authorize(context,"event.delivery.read",resource,audience=()):return row
        if row.get("status")=="received" and not any(row.get(k) for k in ("output","error","session_id","workflow_run_id","task_run_id")):
            return {k:row.get(k) for k in ("id","event_id","status","source","started_at")}
        raise PermissionError("Delivery execution scope is unavailable")

    async def deliveries(self,context,identifier,*,limit=20):
        await self.get(context,identifier)
        rows=await self.db.list_event_deliveries(identifier,limit=2000)
        visible=[]
        for row in rows:
            try:visible.append(await self.visible_delivery(context,row))
            except PermissionError:continue
            if len(visible)>=max(1,min(int(limit),200)):break
        return {"deliveries":visible}

    async def delivery(self,context,identifier):
        await self.authorize(context,"event.read")
        row=await self.db.get_event_delivery(identifier)
        if row is None:raise LookupError("Event delivery not found")
        return await self.visible_delivery(context,row)
