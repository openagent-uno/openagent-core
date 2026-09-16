"""Authenticate original webhook bytes and atomically enqueue a delivery.

The host owns transport authentication and supplies a fresh definition policy.
This service creates no actor, session, run, or execution credential. The host's
automation dispatcher still obtains the original durable delegation at claim.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
import time
from .automation import AutomationRepository
from .webhook_auth import authenticate, extract_external_id, WebhookAuthError

MAX_BODY = 512 * 1024


class WebhookIngressError(Exception):
    def __init__(self, message: str, status: int):
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class WebhookReceipt:
    body: dict
    status: int


class WebhookIngress:
    def __init__(self, db, *, definition_authorized, queue_ready):
        self.db = db
        self.repository = AutomationRepository(db.db_path)
        self.definition_authorized = definition_authorized
        self.queue_ready = queue_ready

    async def receive(self, event_id: str, raw_body: bytes, headers: dict[str, str]) -> WebhookReceipt:
        if len(raw_body) > MAX_BODY:
            raise WebhookIngressError("payload too large", 413)
        if not await self.queue_ready():
            raise WebhookIngressError("unavailable", 503)
        headers = {k.lower(): v for k, v in headers.items()}
        async with self.repository.transaction() as connection:
            db = self.repository.definition_store(connection)
            event = await db.get_event(event_id, include_secret=True)
            if event is None or not event["enabled"]:
                raise WebhookIngressError("not found", 404)
            if len(raw_body) > min(MAX_BODY, int(event.get("max_payload_bytes") or 262144)):
                raise WebhookIngressError("payload too large", 413)
            try:
                authenticate(event=event, raw_body=raw_body, headers=headers, db_path=self.db.db_path)
            except WebhookAuthError:
                raise WebhookIngressError("unauthorized", 401) from None
            if not await self.definition_authorized("event", event):
                raise WebhookIngressError("definition authority unavailable", 403)
            try:
                payload = json.loads(raw_body.decode()) if raw_body else {}
                if not isinstance(payload, dict):
                    payload = {"_body": payload}
            except (ValueError, UnicodeDecodeError):
                payload = {"_raw": raw_body.decode("utf-8", "replace")}
            # Slack retry counts are shared by unrelated events. Its signed
            # event_id is the delivery identity, never X-Slack-Retry-Num.
            external_id = (payload.get("event_id") if event["type"] == "slack" else
                extract_external_id(event=event, headers=headers, payload=payload))
            external_id = external_id or headers.get("idempotency-key") or headers.get("x-delivery-id")
            if external_id is not None and (not isinstance(external_id, str) or not external_id or len(external_id) > 256):
                raise WebhookIngressError("invalid delivery identity", 400)
            if event["type"] == "slack" and payload.get("type") == "url_verification":
                return WebhookReceipt({"challenge": payload.get("challenge", "")}, 200)
            if external_id:
                existing = await (await connection.execute(
                    "SELECT id FROM event_deliveries WHERE event_id=? AND external_id=? ORDER BY started_at LIMIT 1",
                    (event_id, external_id))).fetchone()
                if existing:
                    return WebhookReceipt({"duplicate": True, "delivery_id": existing["id"]}, 200)
            limit = max(1, min(int(event.get("rate_limit_per_min") or 60), 600))
            recent = await (await connection.execute(
                "SELECT count(*) FROM event_deliveries WHERE event_id=? AND started_at>=?",
                (event_id, time.time() - 60))).fetchone()
            if recent[0] >= limit:
                raise WebhookIngressError("rate limited", 429)
            delivery_id = await db.add_event_delivery(event_id=event_id, source="webhook",
                external_id=external_id, payload=payload, claimed=False)
        return WebhookReceipt({"delivery_id": delivery_id, "status": "accepted"}, 202)
