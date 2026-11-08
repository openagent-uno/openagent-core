# Current-user notifications

An embedding host can register a notification sink to expose
`send_notification(title, body)` to agents. The tool writes a private in-app
notification for the invoking user. Hosts that do not register a sink have
no notification tool.

```python
from openagent_core import NotificationCapability

source = NotificationCapability(sink)
catalog.register("notifications", source, source, target_label="Notifications")
```

The ordinary catalog policy applies to discovery, execution and publication.
The capability is available when the run's initiating principal is a user
and is also the run's authority. It requires the exact admitted run context
when invoked. The model supplies a title of at most 120 characters and body
of at most 2,000 characters. Identity, source session and agent come from
the verified execution context. The model cannot address another user or
choose a delivery transport.

Implement `NotificationSink.send(request, context) -> str` in the host. Resolve
the current user from `context.initiator` and recheck authorization against the
current domain state before writing. Persist the record and its idempotency key
before returning its durable ID. The key is stable for identical normalized
content in the same admitted run, including repeated tool calls. On replay the
host returns the same ID without sending again. The tool result contains only
that ID. Host policies own visibility, read state, realtime delivery and any
mobile push fan-out after durable acceptance.
