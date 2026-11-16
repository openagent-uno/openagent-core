# Session goals

A goal turns one conversation into an autonomous task: the agent keeps working
on it, turn after turn, until it is achieved, blocked or out of budget.

## Using it

| Command | Effect |
| --- | --- |
| `/goal <objective>` | Set (or replace) the conversation's goal. The next message starts the work. |
| `/goal` or `/goal status` | Show the goal, its state, continuations used, last note and outcome. |
| `/goal pause` / `/goal resume` | Stop or restart automatic continuation. Resume restores the budget. |
| `/goal done` | Mark the goal achieved yourself. |
| `/goal max <n>` | Change the continuation budget (0–500, default 20). |
| `/goal clear` | Remove the goal. |

The command is in the shared catalogue (`openagent_core.chat_commands`), so it
works in every host that runs session commands (app, CLI, messaging channels).
Hosts can also drive goals through `openagent_core.session_goal.GoalStore`.

## How it works

`openagent_core.session_goal` stores one row per session in `session_goals`
(objective, status `active`/`paused`/`achieved`, `max_continuations`,
`continuations`, note, outcome, pause reason, timestamps). Reads and writes use
a dedicated connection, so a change from a command or an API is visible to the
running turn immediately.

While the goal is active:

- every turn in that session gets a `<session-goal>` block appended to its
  system prompt (stable while the goal is unchanged, so prompt caching holds);
- when the agent loop would end the turn (no pending background shell work),
  `after_reply` takes one continuation from the budget and continues the same
  run in the same session with a short reminder, emitting `iteration_break`
  between iterations like the existing shell autoloop;
- the agent ends the goal by writing `[goal: achieved] <outcome>` on its own
  line, or hands control back with `[goal: blocked] <reason>`. A marker needs no
  tool, so it works with every capability set. Only a line that starts with the
  marker counts; a mention inside a sentence does not;
- when the budget is spent the goal pauses itself with a reason, so an
  unreachable goal never loops forever.

The stop button cancels the running turn as usual; the goal stays active and
continues with the next message. A paused or achieved goal never continues.

A paused goal still reaches the prompt: every turn gets a `<session-goal>`
block that presents the objective (and the pause reason) as the standing
context of the conversation, without the instruction to keep working. So when
the user writes during a pause, the agent still knows its role and rules. An
achieved goal adds nothing to the prompt.
