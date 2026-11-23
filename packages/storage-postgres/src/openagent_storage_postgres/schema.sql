CREATE TABLE IF NOT EXISTS workers (
    id uuid PRIMARY KEY, agent text NOT NULL, expires_at timestamptz NOT NULL);
CREATE INDEX IF NOT EXISTS workers_expiry ON workers(agent,expires_at);
CREATE TABLE IF NOT EXISTS sessions (
    agent text NOT NULL, id text NOT NULL, tenant text NOT NULL, authority text NOT NULL,
    owner_id text NOT NULL, participants text[] NOT NULL, title text, archived boolean NOT NULL DEFAULT false,
    parent_id text, root_id text, origin text, last_activity_at timestamptz NOT NULL DEFAULT clock_timestamp(), created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY (agent,id),
    FOREIGN KEY (agent,parent_id) REFERENCES sessions(agent,id));
CREATE INDEX IF NOT EXISTS sessions_activity ON sessions(agent,tenant,last_activity_at DESC,id DESC);
CREATE TABLE IF NOT EXISTS runs (
    agent text NOT NULL, id text NOT NULL, session_id text NOT NULL, tenant text NOT NULL,
    ordinal bigint GENERATED ALWAYS AS IDENTITY, request_key text NOT NULL, digest text NOT NULL,
    request jsonb NOT NULL, context jsonb NOT NULL, worker uuid NOT NULL,
    status text NOT NULL DEFAULT 'queued', finished_at timestamptz, cancel_requested boolean NOT NULL DEFAULT false,
    output jsonb, parent_run_id text, PRIMARY KEY (agent,id), UNIQUE (agent,session_id,request_key),
    FOREIGN KEY (agent,session_id) REFERENCES sessions(agent,id) ON DELETE CASCADE);
CREATE INDEX IF NOT EXISTS runs_queue ON runs(agent,session_id,ordinal) WHERE status IN ('queued','running');
CREATE INDEX IF NOT EXISTS runs_history ON runs(agent,session_id,ordinal);
CREATE INDEX IF NOT EXISTS runs_owner ON runs(worker) WHERE status IN ('queued','running');
CREATE TABLE IF NOT EXISTS reservations (
    agent text NOT NULL,id text NOT NULL,tenant text NOT NULL,session_id text NOT NULL,participants text[] NOT NULL,PRIMARY KEY(agent,id));
CREATE TABLE IF NOT EXISTS messages (
    seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, id text NOT NULL, agent text NOT NULL, session_id text NOT NULL,
    session_seq bigint NOT NULL CHECK(session_seq>=0), run_id text NOT NULL, role text NOT NULL, author text NOT NULL, body jsonb, call_id text,
    status text NOT NULL DEFAULT 'complete', created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (agent,run_id) REFERENCES runs(agent,id) ON DELETE CASCADE);
CREATE UNIQUE INDEX IF NOT EXISTS messages_id ON messages(agent,id);
CREATE INDEX IF NOT EXISTS messages_run ON messages(agent,run_id,session_seq);
CREATE UNIQUE INDEX IF NOT EXISTS messages_page ON messages(agent,session_id,session_seq);
CREATE TABLE IF NOT EXISTS events (
    seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, agent text NOT NULL,
    run_id text NOT NULL, kind text NOT NULL, payload jsonb NOT NULL,
    FOREIGN KEY (agent,run_id) REFERENCES runs(agent,id) ON DELETE CASCADE);
CREATE INDEX IF NOT EXISTS events_page ON events(agent,run_id,seq);
CREATE TABLE IF NOT EXISTS tool_calls (
    agent text NOT NULL, run_id text NOT NULL, id text NOT NULL, intent jsonb NOT NULL,
    result jsonb, error jsonb, finished boolean NOT NULL DEFAULT false,
    PRIMARY KEY(agent,run_id,id), FOREIGN KEY(agent,run_id) REFERENCES runs(agent,id) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS operations (
    agent text NOT NULL, origin_run_id text NOT NULL, current_run_id text NOT NULL,
    segment integer NOT NULL, state text NOT NULL, PRIMARY KEY(agent,origin_run_id),
    FOREIGN KEY(agent,origin_run_id) REFERENCES runs(agent,id) ON DELETE CASCADE);
ALTER TABLE operations ADD COLUMN IF NOT EXISTS due_at double precision;
CREATE INDEX IF NOT EXISTS operation_due ON operations(agent,due_at)
    WHERE state='active' AND due_at IS NOT NULL;
UPDATE operations d SET due_at=0 FROM runs r WHERE d.agent=r.agent AND d.current_run_id=r.id
    AND d.state='active' AND d.due_at IS NULL AND r.finished_at IS NOT NULL;
CREATE TABLE IF NOT EXISTS handoffs (
    agent text NOT NULL, source_id text NOT NULL, origin_run_id text NOT NULL, PRIMARY KEY(agent,source_id),
    FOREIGN KEY(agent,origin_run_id) REFERENCES operations(agent,origin_run_id) ON DELETE CASCADE);
CREATE INDEX IF NOT EXISTS operation_segments ON runs(agent,((request->'continuation')->>'origin_run_id'))
    WHERE request ? 'continuation';
CREATE UNIQUE INDEX IF NOT EXISTS checkpoint_starts ON events(agent,run_id) WHERE kind='run.operation.started';
CREATE UNIQUE INDEX IF NOT EXISTS checkpoint_resumes ON events(agent,(payload->>'resume_run_id'))
    WHERE kind='run.operation.started' AND payload->>'resume_run_id' IS NOT NULL;

CREATE TABLE IF NOT EXISTS provider_limits (
    provider text PRIMARY KEY, capacity integer NOT NULL CHECK (capacity>0));
CREATE TABLE IF NOT EXISTS provider_tenants (
    provider text NOT NULL REFERENCES provider_limits(provider), authority text NOT NULL, tenant text NOT NULL,
    capacity integer NOT NULL CHECK (capacity>0), PRIMARY KEY(provider,authority,tenant));
CREATE TABLE IF NOT EXISTS provider_accounts (
    provider text NOT NULL REFERENCES provider_limits(provider), account text NOT NULL,
    capacity integer NOT NULL CHECK (capacity>0), cooldown_until timestamptz,
    PRIMARY KEY(provider,account));
CREATE TABLE IF NOT EXISTS provider_attempts (
    id uuid PRIMARY KEY, provider text NOT NULL, account text NOT NULL, worker uuid NOT NULL,
    agent text NOT NULL, run_id text NOT NULL, authority text NOT NULL,tenant text NOT NULL,
    FOREIGN KEY(provider,authority,tenant) REFERENCES provider_tenants(provider,authority,tenant),
    FOREIGN KEY(provider,account) REFERENCES provider_accounts(provider,account),
    FOREIGN KEY(worker) REFERENCES workers(id) ON DELETE CASCADE,
    FOREIGN KEY(agent,run_id) REFERENCES runs(agent,id) ON DELETE CASCADE);
CREATE INDEX IF NOT EXISTS provider_slots ON provider_attempts(provider,account);
CREATE INDEX IF NOT EXISTS provider_slots_worker ON provider_attempts(worker);
