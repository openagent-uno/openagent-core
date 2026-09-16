export interface RunRequest {
  run_id: string;
  session_id: string;
  idempotency_key: string;
  input: string;
  deadline_seconds?: number | null;
  model_ref?: string | null;
  steer_run_id?: string | null;
  attachments?: ReadonlyArray<Record<string, unknown>>;
}
export interface RunRecord {
  run_id: string;
  session_id: string;
  tenant_id: string;
  status: string;
  request_digest: string;
  cancel_requested: boolean;
  output?: unknown;
}
export interface RunEvent {
  cursor: number;
  run_id: string;
  kind: string;
  payload: Record<string, unknown>;
}
const terminal = new Set(["success", "failed", "cancelled", "rejected", "interrupted", "skipped", "timed_out"]);
export class RunHTTPError extends Error {
  constructor(readonly status: number) { super(`Run service returned HTTP ${status}`); }
}
export class AcceptanceUncertain extends Error {
  constructor(readonly runId: string, options?: ErrorOptions) {
    super("Acceptance is uncertain; reconcile this same run ID", options);
  }
}

export class RunClient {
  private base: string;
  constructor(baseURL: string, private headers: () => HeadersInit | Promise<HeadersInit>,
              path = "/api/v1/runs", private timeoutMs = 10000) {
    const origin = new URL(baseURL);
    if (!["http:", "https:"].includes(origin.protocol) || origin.username || origin.password)
      throw new Error("An explicit HTTP service origin is required");
    this.base = baseURL.replace(/\/$/, "") + path.replace(/\/$/, "");
  }
  private async request<T>(method: string, path = "", body?: unknown, signal?: AbortSignal): Promise<T> {
    const headers = new Headers(await this.headers());
    headers.set("Accept", "application/json");
    if (body !== undefined) headers.set("Content-Type", "application/json");
    const controller = new AbortController();
    const abort = () => controller.abort(signal?.reason);
    if (signal?.aborted) abort();
    signal?.addEventListener("abort", abort, {once: true});
    const timer = setTimeout(() => controller.abort(new Error("Request timeout")), this.timeoutMs);
    try {
      const response = await fetch(this.base + path, {
        method, headers, body: body === undefined ? undefined : JSON.stringify(body),
        redirect: "error", signal: controller.signal,
      });
      if (!response.ok) throw new RunHTTPError(response.status);
      return await response.json() as T;
    } finally {
      clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
    }
  }
  async submit(input: RunRequest, signal?: AbortSignal): Promise<RunRecord> {
    // Snapshot the request before awaiting refreshed authentication headers.
    const snapshot = structuredClone(input);
    try { return await this.request("POST", "", snapshot, signal); }
    catch (cause) {
      if (cause instanceof RunHTTPError && cause.status < 500) throw cause;
      throw new AcceptanceUncertain(snapshot.run_id, {cause});
    }
  }
  get(runId: string, signal?: AbortSignal): Promise<RunRecord> {
    return this.request("GET", "/" + encodeURIComponent(runId), undefined, signal);
  }
  async events(runId: string, after = 0, signal?: AbortSignal): Promise<RunEvent[]> {
    if (!Number.isSafeInteger(after) || after < 0) throw new Error("Invalid event cursor");
    const page = await this.request<{events: RunEvent[]}>("GET", "/" + encodeURIComponent(runId) + "/events?after=" + after, undefined, signal);
    return page.events;
  }
  cancel(runId: string, signal?: AbortSignal): Promise<RunRecord> {
    return this.request("POST", "/" + encodeURIComponent(runId) + "/cancel", undefined, signal);
  }
  async children(runId: string, signal?: AbortSignal): Promise<RunRecord[]> {
    const result = await this.request<{runs: RunRecord[]}>("GET", "/" + encodeURIComponent(runId) + "/children", undefined, signal);
    return result.runs;
  }
  async wait(runId: string, signal?: AbortSignal, intervalMs = 500): Promise<RunRecord> {
    if (intervalMs <= 0) throw new Error("Polling interval must be positive");
    for (;;) {
      const record = await this.get(runId, signal);
      if (terminal.has(record.status)) return record;
      await new Promise<void>((resolve, reject) => {
        const abort = () => { clearTimeout(timer); reject(signal?.reason ?? new Error("Aborted")); };
        const timer = setTimeout(() => { signal?.removeEventListener("abort", abort); resolve(); }, intervalMs);
        if (signal?.aborted) abort();
        else signal?.addEventListener("abort", abort, {once: true});
      });
    }
  }
}
