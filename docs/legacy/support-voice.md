# Customer support voice

The controller determines account authority, product facts, evidence, operations,
and the next useful question. These become an internal operational brief. They
are not a customer-facing fallback.

With `OPENAGENT_SUPPORT_HUMAN_VOICE=1` (the production default), every substantive
controller reply is written using the latest message and recent conversation.
The writer acknowledges the person's actual experience, explains useful next steps,
and preserves all relevant facts without pressure, exaggerated empathy, invented
promises or claims of being a human. Cancellation and account deletion receive the
same care without retention friction.

A separate model call checks factual support, complete coverage, relevance,
language and tone. Each required point must have a verbatim supporting quote in
the proposed reply. Numeric and authority checks remain deterministic. Findings
request up to two bounded rewrites. Failure or timeout queues human review and sends no
canned text. A hash binds approval to the exact bytes handed to delivery; a later
guard cannot silently substitute its own wording.

The writer/reviewer can use an enabled registered model through
`OPENAGENT_SUPPORT_VOICE_MODEL`; otherwise the event model is used. No provider is
hardcoded. `OPENAGENT_SUPPORT_VOICE_TIMEOUT_SECONDS` defaults to 45 seconds per
call. A shared 120-second deadline covers the entire voice cycle, including later
guard retries, within the production event's 240-second limit. Tool access remains disabled for writing/review, with the existing model
concurrency gate. Public replies respect actual channel limits; private replies
have space for a complete explanation.

Commercial and entitlement terms come from the host's authorized rules and
current evidence. The writer keeps plan benefits, optional rewards and account
state distinct. It handles a reported defect using the evidence for that case.
Lean local events use the host's [account-state evidence setting](../USAGE.md#account-state-evidence-for-lean-event-replies).

Operational unit doubles run with the voice feature disabled to preserve their
isolated planning contracts. The `support_voice` tests explicitly enable it and
cover writer/reviewer, context, bounded repair, failure/hold, authority checks,
quote coverage and delivery integrity. Model replay always enables the feature;
use the same registered voice model as production and inspect the actual replies.

Guidance receives the retrieved product references and operator policy instead of
a generic fallback question. Product steps require supporting source quotations;
ordinary definitions can explain unfamiliar terms without claiming app-specific
capture or navigation behaviour. Reported versions can be acknowledged as context;
they never establish account or payment state. Prose is written directly rather
than embedded in JSON, avoiding broken escaping around quoted UI labels.

Post-deployment regressions also cover direct requested behaviour, MCP-wrapped
source text and attachments, and repeated attachment follow-ups. Wrapped source
whitespace is normalized for quote matching, while invented quotations still fail.
Guidance uses the same writer/reviewer directly rather than spending a separate
pair of model calls before writing it. Its product references remain available
when an attempted documented bug resolution was not sufficient.

A reclaimed human handoff requires a linked task. On that explicit rejection
the controller calls the host's idempotent task operation, verifies the
persisted thread link, and retries the handoff once. Existing review holds remain
in place; they can gain ownership without sending another bot message. Unknown
creation outcomes are not retried blindly. A task is not proof a person has read
it or that the customer problem is resolved.

Event history retains valid compact JSON with the delivery result, hold category,
writer failures and ownership outcome even when raw tool receipts exceed the
history budget. Technical event success remains distinct from sent/blocked/held.

The historical multi-turn replay uses simulated business operations.
An embedding host separately qualifies its own reply and delivery guards.
