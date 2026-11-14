# Channel messages

An embedding host can register a channel message sink to let agents post on
their own initiative in messaging conversations the host admits, for example a
report with files in a team Slack channel. Replies to the conversation that
started a run keep using the ordinary channel bridge reply path.

```python
from openagent_core import ChannelMessageCapability

source = ChannelMessageCapability(sink)
catalog.register("channel_messages", source, source, target_label="Channels")
```

Agents see two tools:

- `list_channel_destinations()` returns `{id, label, provider}` for each
  conversation this run may use.
- `send_channel_message(destination, text?, files?)` posts text (at most 12,000
  characters) and up to 10 files. Files are artifact references returned by
  the agent's file tools, such as `publish_file`.

Implement `ChannelMessageSink` in the host. `destinations(context)` returns the
admitted conversations after fresh domain checks; the runtime refuses a
destination that is not in that fresh list. `send(request, context)` rechecks
authorization, resolves file references only in the run's own artifact scope,
publishes once per `idempotency_key` (stable for identical content in the same
run) and returns a durable ID. The model cannot choose a provider credential,
an actor or a conversation outside the list.

For channel bridges, the `channel-bridge-ipc/v1` host methods `outbound_open`
and `deliver` publish as the binding's own bot in an admitted conversation; see
the channel bridges package documentation.
