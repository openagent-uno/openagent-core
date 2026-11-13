# Model stream recovery

`acall_model_stream_with_fallback` decides what happens when a provider stream
fails. The order is fixed and every step that resends the request runs only
while the failed attempt has emitted no event, so a turn never sees duplicated
output.

1. **Transient retry.** A failure with no status code or a 5xx status (a
   dropped connection, a stream that errored before its first event) is retried
   once on the same model after one second. Rate limits, context-window
   overflows and other 4xx errors are not retried here. A retry that emits
   output and then fails propagates like any mid-stream failure.
2. **In-place recovery** of known, deterministically repairable requests.
3. **Credential-pool rotation** for rate-limited pooled credentials.
4. **Configured fallback models** (`FallbackConfig`).

Without step 1 a single network interruption ended a long agent turn, because
models default to `retries = 0` and many installations configure no fallback.
Regressions: `tests/public/test_transient_stream_retry.py`.

## Parallel local tool calls

On a connected computer, mutating tools share a single-writer lease
(`openagent_capability_host.lease.MutatingLease`). A call from the principal
that already holds the lease now waits up to 30 seconds for it, so a model's
parallel calls are serialized instead of the second failing with `lease_held`
("another local tool call is currently mutating this computer"). A different
principal is still refused immediately. Regressions:
`tests/public/test_capability_lease_wait.py`.
