"""Add cross-session delegation beside the legacy intra-session run ancestry.

The existing parent_run_id foreign key intentionally stays unchanged. No table,
legacy row, message ID or historical owner is reconstructed by this migration.
"""

def ensure_delegated_ancestry(db):
    columns={row[1] for row in db.execute('PRAGMA table_info(session_runs)')}
    if 'delegated_parent_run_id' not in columns:
        db.execute('ALTER TABLE session_runs ADD COLUMN delegated_parent_run_id TEXT REFERENCES session_runs(id) ON DELETE SET NULL')
    db.execute('CREATE INDEX IF NOT EXISTS idx_session_runs_delegated_parent ON session_runs(delegated_parent_run_id) WHERE delegated_parent_run_id IS NOT NULL')
    for operation in ('INSERT','UPDATE OF delegated_parent_run_id, tenant_id'):
        suffix='insert' if operation=='INSERT' else 'update'
        db.execute(f'''CREATE TRIGGER IF NOT EXISTS runtime_delegated_parent_tenant_{suffix}
            BEFORE {operation} ON session_runs
            WHEN NEW.delegated_parent_run_id IS NOT NULL AND NOT EXISTS
                (SELECT 1 FROM session_runs p WHERE p.id=NEW.delegated_parent_run_id AND p.tenant_id=NEW.tenant_id)
            BEGIN SELECT RAISE(ABORT,'Delegated parent belongs to a different tenant'); END''')
    # Earlier migration snapshots already stored the exact relation in their
    # immutable admission evidence. Only verified same-tenant parents qualify.
    db.execute('''UPDATE session_runs AS child
        SET delegated_parent_run_id=json_extract(child.metadata_json,'$.execution_context.parent_run_id')
        WHERE child.delegated_parent_run_id IS NULL
          AND json_extract(child.metadata_json,'$.runtime_contract')=1
          AND EXISTS (SELECT 1 FROM session_runs parent
              WHERE parent.id=json_extract(child.metadata_json,'$.execution_context.parent_run_id')
                AND parent.tenant_id=child.tenant_id)''')
