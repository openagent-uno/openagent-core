# Shared chat commands

`openagent_core.chat_commands` is the single definition of the slash commands:
their catalogue (`COMMAND_SPECS`) and their semantics. Bridges, the standalone
gateway and embedding hosts inherit them. A host does not reimplement a command.

## Host contract

A host authenticates the caller and implements `ChatCommandHost`, only for what
Core cannot see. Every method is optional, and a missing method makes the
commands needing it report `unsupported` instead of acknowledging a no-op.

| Method | Purpose |
|---|---|
| `stop_session` | Stop the running turn, drop queued input |
| `rotate_session` | Make the next turn start in a fresh session |
| `session_status` | Busy flag, queue depth, session count |
| `authorize_model_change` | Host permission for `/model <id>` |
| `forget_history` | Override how history is forgotten (non-retaining hosts) |

The session model stays the host's. `ChatCommandService.execute` is the entry
point for a host in the engine's process. `Runtime.chat_command` adds session and
policy verification for an embedded runtime and runs beside the session's turns,
so `/stop` does not wait for the turn it stops, and adds no transcript input.

A host in another process uses `DeferredChatCommandHost`. It sends a snapshot and
receives `host_actions`, currently `stop_runs` and `rotate_session`. It must apply
all of them in order and show the result only if they all succeed. It learns no
command names, so a new command needs no host change.

## Semantics

- **`new`, `clear`, `reset`:** stop the running turn, drop the queue, rotate the
  session. With `retains_history = True` only live resources are released, and the
  previous conversation stays readable and resumable. Otherwise provider resume
  state and stored model history are forgotten.
- **Shared conversations cannot be reset:** one member must not erase the others'
  context. The check comes first, so a refused reset stops nothing.
- **Never touched:** long-term memory, user memories and the vault.
- **`stop`:** stops the running turn and drops the queue, keeping the context.
- **`status`, `queue`:** read the host's state.
- **`help`:** lists the commands of the host's scopes.
- **`compact`, `context`, `model`:** use `SessionControl` and the context report.
- **`usage`, `update`, `restart`:** `instance` scope. They administer an installation
  and stay with a host that owns one.

A result has `status` `completed`, `unsupported` or `failed`. Clients must not show
success for anything but `completed`. `expose_errors = False` withholds exception
text from a result shown on a third-party channel.
