export interface RunRequest {
  run_id: string;
  session_id: string;
  idempotency_key: string;
  input: string;
  deadline_seconds?: number | null;
  admission_policy?: "defer-unready";
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
  origin_run_id?: string;
  segment_run_id?: string;
  segment?: number;
}
export interface RunEvent {
  cursor: number;
  run_id: string;
  kind: string;
  payload: Record<string, unknown>;
}
export interface RunEventPage {
  events: RunEvent[];
  cursor: number;
  has_more: boolean;
}
export interface RunFinalAnswer {
  run_id: string;
  segment_run_id: string;
  final_answer: Record<string, unknown> | null;
}
const terminal = new Set(["success", "failed", "cancelled", "rejected", "interrupted", "skipped", "timed_out"]);
export class RunHTTPError extends Error {
  constructor(readonly status: number) { super(`Run service returned HTTP ${status}`); }
}
export class RunAdmissionPending extends Error {
  constructor(readonly retryAfterSeconds: number) { super("Run admission is waiting for a host prerequisite"); }
}
export class AcceptanceUncertain extends Error {
  constructor(readonly runId: string, options?: ErrorOptions) {
    super("Acceptance is uncertain; reconcile this same run ID", options);
  }
}

class HttpClient {
  private base: string;
  constructor(baseURL: string, private headers: () => HeadersInit | Promise<HeadersInit>,
              path = "/api/v1/runs", private timeoutMs = 10000) {
    const origin = new URL(baseURL);
    if (!["http:", "https:"].includes(origin.protocol) || origin.username || origin.password)
      throw new Error("An explicit HTTP service origin is required");
    this.base = baseURL.replace(/\/$/, "") + path.replace(/\/$/, "");
  }
  protected async request<T>(method: string, path = "", body?: unknown, signal?: AbortSignal): Promise<T> {
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
      if (method === "POST" && path === "" && response.status === 503 && response.headers.has("Retry-After")) {
        const detail = await response.json().catch(() => null);
        const delay = detail?.retry_after_seconds;
        if (detail?.error === "run_admission_pending" && Number.isSafeInteger(delay) && delay >= 1 && delay <= 3600
            && response.headers.get("Retry-After") === String(delay)) throw new RunAdmissionPending(delay);
      }
      if (!response.ok) throw new RunHTTPError(response.status);
      return await response.json() as T;
    } finally {
      clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
    }
  }
}

export interface SessionSummary {
  session_ref: string;
  session_id: string;
  title: string | null;
  archived: boolean;
}
export interface MemorySummary { space?: string; visibility?: "personal" | "shared"; path: string; title: string | null; revision: number; }
export interface CatalogOptions { query?: string; cursor?: string; limit?: number; }

export class PersonalClient extends HttpClient {
  constructor(baseURL: string, headers: () => HeadersInit | Promise<HeadersInit>,
              path = "/api/v1/me", timeoutMs = 10000) {
    super(baseURL, headers, path, timeoutMs);
  }
  private catalog<T>(resource: string, options: CatalogOptions, detail: Record<string, string>,
                     signal?: AbortSignal): Promise<T> {
    const params = new URLSearchParams(detail);
    if (options.query !== undefined) params.set("q", options.query);
    if (options.cursor !== undefined) params.set("cursor", options.cursor);
    if (options.limit !== undefined) params.set("limit", String(options.limit));
    return this.request("GET", "/" + resource + "?" + params, undefined, signal);
  }
  sessions(options: CatalogOptions = {}, signal?: AbortSignal) {
    return this.catalog<{sessions: SessionSummary[]; next_cursor: string | null}>("sessions", options, {}, signal);
  }
  session(ref: string, options: Omit<CatalogOptions, "query"> = {}, signal?: AbortSignal) {
    return this.catalog<{session: SessionSummary; messages: Record<string, unknown>[]; next_cursor: string | null}>(
      "sessions", options, {ref}, signal);
  }
  memories(options: Omit<CatalogOptions, "query"> & {space?: string} = {}, signal?: AbortSignal) {
    return this.catalog<{notes: MemorySummary[]; next_cursor: string | null}>("memories", options, options.space ? {space: options.space} : {}, signal);
  }
  searchMemories(query: string, options: Omit<CatalogOptions, "query"> & {space?: string} = {}, signal?: AbortSignal) {
    return this.catalog<{hits: MemorySummary[]; next_cursor: string | null}>("memories", {...options, query}, options.space ? {space: options.space} : {}, signal);
  }
  memory(path: string, signal?: AbortSignal, space?: string) {
    return this.catalog<MemorySummary & {content: string}>("memories", {}, space ? {path, space} : {path}, signal);
  }
}

export class RunClient extends HttpClient {
  async submit(input: RunRequest, signal?: AbortSignal): Promise<RunRecord> {
    // Snapshot the request before awaiting refreshed authentication headers.
    const snapshot = structuredClone(input);
    try { return await this.request("POST", "", snapshot, signal); }
    catch (cause) {
      if (cause instanceof RunAdmissionPending || (cause instanceof RunHTTPError && cause.status < 500)) throw cause;
      throw new AcceptanceUncertain(snapshot.run_id, {cause});
    }
  }
  cancelUnadmitted(runId: string, sessionId: string, signal?: AbortSignal): Promise<RunRecord> {
    return this.request("POST", "/" + encodeURIComponent(runId) + "/cancel-unadmitted", {session_id: sessionId}, signal);
  }
  cancelInSession(runId: string, sessionId: string, signal?: AbortSignal): Promise<RunRecord> {
    return this.request("POST", "/" + encodeURIComponent(runId) + "/cancel", {session_id: sessionId}, signal);
  }
  get(runId: string, signal?: AbortSignal): Promise<RunRecord> {
    return this.request("GET", "/" + encodeURIComponent(runId), undefined, signal);
  }
  async events(runId: string, after = 0, signal?: AbortSignal): Promise<RunEvent[]> {
    return (await this.eventsPage(runId, after, 1000, signal)).events;
  }
  eventsPage(runId: string, after = 0, limit = 1000, signal?: AbortSignal): Promise<RunEventPage> {
    if (!Number.isSafeInteger(after) || after < 0) throw new Error("Invalid event cursor");
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000) throw new Error("Invalid event limit");
    const path = "/" + encodeURIComponent(runId) + "/events?after=" + after;
    return this.request("GET", limit === 1000 ? path : path + "&limit=" + limit, undefined, signal);
  }
  async *iterEvents(runId: string, after = 0, limit = 1000, signal?: AbortSignal): AsyncGenerator<RunEvent> {
    for (;;) {
      const page = await this.eventsPage(runId, after, limit, signal);
      if (page.has_more && (!Number.isSafeInteger(page.cursor) || page.cursor <= after)) {
        throw new Error("Run event page did not advance its cursor");
      }
      yield* page.events;
      if (!page.has_more) return;
      after = page.cursor;
    }
  }
  finalAnswer(runId: string, signal?: AbortSignal): Promise<RunFinalAnswer> {
    return this.request("GET", "/" + encodeURIComponent(runId) + "/final-answer", undefined, signal);
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

export {
  classifyModelFailure,
  reconcileTurnCompletion,
  type CommittedEffect,
  type ModelFailureKind,
  type TurnCompletion,
} from './host-turn.js';
