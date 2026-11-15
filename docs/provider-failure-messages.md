# Provider failure messages

A failed run carries a stable public code from
`openagent_core.core.public_errors`; hosts and channel bridges show
`PUBLIC_MESSAGES[code]` and never the provider's diagnostic. The durable run
record stores `error_code` and `error_public` next to `error_type`.

| Code | Cause | Retry helps? |
| --- | --- | --- |
| `credit_exhausted` | HTTP 402, "insufficient balance", "payment required", "credit balance is too low" | No: recharge the provider account or choose another model |
| `quota_exhausted` | Usage quota or allowance exhausted (`insufficient_quota`, usage limit) | Not until the quota resets |
| `request_too_large` | HTTP 413 or a request-body size limit of the model endpoint | No: shorten the conversation, drop attachments or choose another model |
| `context_length` | The model's context window is exceeded | No |
| `rate_limit` | Temporary request-rate limit (HTTP 429) | Yes, after the interval |

HTTP 402 and 413 are recognised by status code whatever exception wraps
them, including a rate-limit error raised by a retry layer, and by the
equivalent wording when the status survives only in the text.

`NON_RETRYABLE_CODES`, `is_retryable(code)` and `RunErrorClass.retryable`
tell a host whether to offer a retry; the messages of non-retryable codes
say in words that retrying will not help.
