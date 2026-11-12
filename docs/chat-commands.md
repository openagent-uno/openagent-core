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
| `release_resources` | Release live provider resources after stopping turns (retaining hosts) |
| `session_status` | Busy flag, queue depth, session count |
| `authorize_model_change` | Host permission for `/model <id>` |
| `forget_history` | Override how history is forgotten (non-retaining hosts) |

The session model stays the host's. `ChatCommandService.execute` is the entry
point for a host in the engine's process. `Runtime.chat_command` adds session and
policy verification for an embedded runtime and runs beside the session's turns,
so `/stop` does not wait for the turn it stops, and adds no transcript input.

A host in another process uses `DeferredChatCommandHost`. It sends a snapshot and
receives `host_actions`, currently `stop_runs`, `release_session` and `rotate_session`. It must apply
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

## Local review, 1 November 2026

A deferred reset plans **stop_runs → release_session → rotate_session** without
closing a provider while the host is still stopping its run. The host applies
resource release through `Runtime.chat_command_action`, with the original
verified actor, old session and operation ID. Core authorizes it again and takes
the same session lock as real execution. A ten-second bound includes draining
and release; a timeout/failure must stop the plan before rotation. This releases
live resources only, retaining stored conversations, memory and vault.

Resource release is idempotent for that old session. A durable host must claim
the entire command before choosing the session, bind the provider event to its
payload and authority, and retain its result. Claiming rotation alone is unsafe:
a redelivery could stop a newly created conversation. An uncertain pending claim
must not be rerun automatically. A synchronous host retains its existing order.

Local focused qualification: 21 tests and 3 subtests across chat commands and
engine execution. One test uses a real Runtime/SQLite store, a running and queued
run, delayed provider cancellation, and a provider that refuses release while
active. It verifies bounded failed draining, authorization, retained old run
records and an independent fresh session. These are synthetic model/provider
checks; no public wheel or real channel is qualified by them.

## Slash input arriving as an admitted run

Use `AgentExecutor.execute_chat_command` to interpret slash input through the
same catalogue. It rechecks publication authority and returns `None` for normal
or multiline input. Supply a host port only for domain authorization. The
default deferred host cannot stop/rotate its own admitted run and reports those
operations as unsupported. Interactive command endpoints use
`Runtime.chat_command` outside the run queue, applying the shared host actions.

## Noninteractive Windows subprocesses

The engine adds the inherited critical-error and fault-error mode before starting
MCP stdio children, preserving existing flags. This prevents modal error paths
without modifying the MCP SDK installation. Cleanup jobs and sandbox ownership
remain with their transport/host. See Microsoft's [process error-mode contract](https://learn.microsoft.com/en-us/windows/win32/api/errhandlingapi/nf-errhandlingapi-seterrormode).


## Explicit stop terminal boundary

A stream Interrupt owns its cancelled terminal event independently of the
model runner. It suppresses that runner's terminal frames during cancellation
and publishes exactly one cancelled completion after the bounded drain.
A task cancelled before its first instruction therefore cannot leave a batched
channel collector attached to the next turn. Typed-input burst coalescence
keeps its existing separate completion suppression.
