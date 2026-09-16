"""Host-authorized operational log views, with no prompt/credential payloads."""
from __future__ import annotations
import asyncio
import re
from .contracts import ResourceRef,require_authorized
from .runtime import runtime_scope


class LogAdministration:
    def __init__(self,runtime,authorizer):self.runtime,self.authorizer=runtime,authorizer

    def checked_path(self):
        root=self.runtime.settings.workspace.resolve();path=root/"logs"/"events.jsonl"
        if not path.resolve().is_relative_to(root):raise PermissionError("Log path escapes the host workspace")
        return path

    async def authorize(self,context,action):
        await require_authorized(self.authorizer,context,action,ResourceRef("agent",context.tenant_id,context.agent_id))

    async def read(self,context,*,lines=100,event=None):
        await self.authorize(context,"logs.read")
        if not 1<=int(lines)<=1000 or event is not None and (not isinstance(event,str) or len(event)>160):raise ValueError("Invalid log query")
        self.checked_path()
        from .core.logging import read_tail
        with runtime_scope(self.runtime):rows=await asyncio.to_thread(read_tail,int(lines),event)
        result=[]
        for row in rows:
            session=row.get("session_id") or row.get("sessionId")
            if session:
                try:await require_authorized(self.authorizer,context,"session.read",ResourceRef("session",context.tenant_id,str(session)))
                except PermissionError:continue
            # Provider errors and tool payloads may contain secrets or private
            # corpus text. Publish a deliberate operational projection, never
            # a best-effort replacement across arbitrary nested payloads.
            name=str(row.get("event") or "runtime.event")
            if not re.fullmatch(r"[A-Za-z0-9_.-]{1,160}",name):name="runtime.event"
            item={"event":name,"ts":row.get("ts") if isinstance(row.get("ts"),(int,float)) else 0,
                "level":row.get("level") if row.get("level") in {"debug","info","warning","error","critical"} else "info"}
            for key in ("duration_ms","elapsed_ms","duration_s","count","retry","attempt","exit_code","tokens","input_tokens","output_tokens"):
                if isinstance(row.get(key),(int,float)):item[key]=row[key]
            if session:item["session_id"]=session
            for key in ("status","error_type"):
                value=row.get(key)
                if isinstance(value,str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,80}",value):item[key]=value
            result.append(item)
        await self.authorize(context,"logs.read")
        return result

    async def clear(self,context):
        await self.authorize(context,"logs.write")
        self.checked_path()
        from .core.logging import clear
        with runtime_scope(self.runtime):await asyncio.to_thread(clear)
        return {"ok":True}
