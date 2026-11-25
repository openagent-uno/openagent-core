/** Shared decisions for hosts that adapt provider failures and run completion. */
export type ModelFailureKind =
  | 'quota_exhausted'
  | 'unsupported_tool_choice'
  | 'transient'
  | 'rejected';

function record(value: unknown): Record<string, unknown> | undefined {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined;
}

export function classifyModelFailure(status: number, body: string): ModelFailureKind {
  // Consult only the provider error envelope. Some gateways return plain text
  // instead of JSON, so retain the same narrow checks for that format.
  let failure: Record<string, unknown> | undefined;
  let json = false;
  try {
    const envelope = record(JSON.parse(body));
    failure = record(envelope?.error) ?? envelope;
    json = true;
  } catch {
    // Use the plain-text response below.
  }
  const code = String(failure?.code ?? failure?.type ?? '').toLowerCase();
  const message = (typeof failure?.message === 'string' ? failure.message : json ? '' : body).toLowerCase();
  const unsupported = /\b(?:unsupported|not supported|does not support|not available|invalid)\b/;

  // Z.ai uses 429/1113 for an exhausted balance. Other OpenAI-compatible
  // providers may use the same HTTP status for genuine request throttling.
  if (status === 402 || (status === 429 && (
    ['1113', 'insufficient_quota', 'insufficient_balance', 'billing_hard_limit_reached'].includes(code) ||
    /\binsufficient balance\b|\bno resource package\b|\bbilling hard limit\b/.test(message)
  ))) return 'quota_exhausted';

  if ((status === 400 || status === 422) && (
    code === 'unsupported_tool_choice' ||
    (failure?.param === 'tool_choice' && (code === 'unsupported_value' || unsupported.test(message))) ||
    (/\btool[_ -]?choice\b/.test(message) && unsupported.test(message))
  )) return 'unsupported_tool_choice';

  if (status === 429 || status >= 500 && status <= 599) return 'transient';
  return 'rejected';
}

export interface CommittedEffect<T> {
  /** This state comes from a committed receipt or its trusted host adapter. */
  state: 'confirmed' | 'uncertain' | 'rejected' | 'pending';
  value: T;
}

export type TurnCompletion<T> =
  | { kind: 'answer'; text: string }
  | { kind: 'effects'; effects: readonly CommittedEffect<T>[] }
  | { kind: 'missing' };

/** With no final prose, preserve evidence of confirmed or unresolved effects.
 * A rejected tool alone never supports a success or an uncertain-effect reply. */
export function reconcileTurnCompletion<T>(
  finalText: string | null,
  receipts: readonly CommittedEffect<T>[],
): TurnCompletion<T> {
  if (finalText?.trim()) return { kind: 'answer', text: finalText };
  const effects = receipts.filter(receipt => receipt.state !== 'rejected');
  return effects.length ? { kind: 'effects', effects } : { kind: 'missing' };
}
