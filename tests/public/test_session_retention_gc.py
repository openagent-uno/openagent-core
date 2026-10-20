"""The retention job reclaims tombstoned payloads without an online VACUUM.

Covers the stdlib-only ``openagent_core.core.session_retention`` module:
tombstoned v2 sessions lose message/tool rows and run payloads while their
identities stay for lineage, consumed outbox deletes lose their source history
(never the head row or anything a slower consumer still needs), size trimming
keeps the newest run, in-flight work is never pruned, and the recurring job
never compacts the file.
"""
from pathlib import Path
import json
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from openagent_core.core import session_retention as sr


OUTBOX_SCHEMA = '''
  CREATE TABLE search_outbox(seq INTEGER PRIMARY KEY AUTOINCREMENT, tenant_id TEXT NOT NULL, source_kind TEXT NOT NULL,
    source_id TEXT NOT NULL, operation TEXT NOT NULL CHECK (operation IN ('upsert','delete','acl_change','rebuild')),
    source_version INTEGER NOT NULL, acl_version INTEGER NOT NULL, committed_at_ms INTEGER NOT NULL,
    UNIQUE (tenant_id, source_kind, source_id, operation, source_version, acl_version)) STRICT;
  CREATE INDEX idx_search_outbox_source ON search_outbox(tenant_id, source_kind, source_id, seq);
  CREATE TRIGGER trg_search_outbox_no_update BEFORE UPDATE ON search_outbox
    BEGIN SELECT RAISE(ABORT, 'search_outbox rows are immutable'); END;
  CREATE TABLE search_outbox_consumers(consumer_id TEXT PRIMARY KEY, last_seq INTEGER NOT NULL) STRICT;
'''

RUNS_SCHEMA = '''
  CREATE TABLE sessions(session_id TEXT PRIMARY KEY, session_type TEXT, updated_at INTEGER, runs TEXT);
  CREATE TABLE sessions_v2(id TEXT PRIMARY KEY, deleted_at_ms INTEGER);
  CREATE TABLE task_runs(session_id TEXT, status TEXT);
  CREATE TABLE session_runs(id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions_v2(id) ON DELETE CASCADE,
    status TEXT NOT NULL, input_json TEXT CHECK (input_json IS NULL OR json_valid(input_json)),
    output_json TEXT CHECK (output_json IS NULL OR json_valid(output_json)),
    metrics_json TEXT CHECK (metrics_json IS NULL OR json_valid(metrics_json)),
    metadata_json TEXT NOT NULL DEFAULT '{}',
    raw_envelope_json TEXT NOT NULL CHECK (json_valid(raw_envelope_json) AND json_type(raw_envelope_json)='object'),
    legacy_raw_json TEXT CHECK (legacy_raw_json IS NULL OR json_valid(legacy_raw_json))) STRICT;
'''


def add_outbox(conn, rows):
    for kind, source_id, operation in rows:
        conn.execute(
            'INSERT INTO search_outbox(tenant_id,source_kind,source_id,operation,source_version,acl_version,'
            'committed_at_ms) VALUES (?,?,?,?,(SELECT count(*)+1 FROM search_outbox),1,0)',
            ('t', kind, source_id, operation),
        )


class DeletedSessionPayloads(unittest.TestCase):
    def test_only_tombstoned_unrestored_payloads_are_reclaimed(self):
        conn = sqlite3.connect(':memory:')
        conn.execute('PRAGMA foreign_keys=ON')
        conn.executescript('''
          CREATE TABLE sessions(session_id TEXT PRIMARY KEY);
          CREATE TABLE sessions_v2(id TEXT PRIMARY KEY, deleted_at_ms INTEGER);
          CREATE TABLE session_messages(id TEXT PRIMARY KEY, session_id TEXT REFERENCES sessions_v2(id), text TEXT);
          CREATE TABLE tool_invocations(id TEXT PRIMARY KEY, session_id TEXT REFERENCES sessions_v2(id), result_text TEXT);
          CREATE TABLE parts(id TEXT PRIMARY KEY, message_id TEXT REFERENCES session_messages(id) ON DELETE CASCADE);
        ''')
        for session_id, deleted in [('deleted', 1), ('active', None), ('native-v2', None), ('restored', 1)]:
            conn.execute('INSERT INTO sessions_v2 VALUES (?,?)', (session_id, deleted))
            for index in range(5):
                row_id = f'{session_id}{index}'
                conn.execute('INSERT INTO session_messages VALUES (?,?,?)', (row_id, session_id, 'x' * 2000))
                conn.execute('INSERT INTO tool_invocations VALUES (?,?,?)', (row_id, session_id, 'x' * 2000))
                conn.execute('INSERT INTO parts VALUES (?,?)', (row_id, row_id))
        # A legacy row means the session was restored after the tombstone.
        conn.execute('INSERT INTO sessions VALUES (?)', ('restored',))
        conn.commit()

        result = sr.prune_deleted_payloads(conn, batch_size=2)

        self.assertEqual(result, {'tool_invocations': 5, 'session_messages': 5})
        self.assertEqual(conn.execute('SELECT count(*) FROM parts').fetchone()[0], 15)
        self.assertEqual(conn.execute('SELECT count(*) FROM sessions_v2').fetchone()[0], 4)
        self.assertEqual(conn.execute('PRAGMA foreign_key_check').fetchall(), [])
        self.assertEqual(sr.prune_deleted_payloads(conn), {'tool_invocations': 0, 'session_messages': 0})

    def test_legacy_database_remains_supported(self):
        conn = sqlite3.connect(':memory:')
        conn.execute('CREATE TABLE sessions(session_id TEXT PRIMARY KEY)')
        self.assertEqual(sr.prune_deleted_payloads(conn), {})


class TranscriptRetention(unittest.TestCase):
    def test_size_trim_preserves_newest_run_and_does_not_vacuum(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / 'db')
            conn = sqlite3.connect(path)
            conn.execute('CREATE TABLE sessions(session_id TEXT PRIMARY KEY, session_type TEXT, '
                         'updated_at INTEGER, runs TEXT)')
            runs = [{'text': 'a' * 1000}, {'text': 'b' * 1000}, {'text': 'newest'}]
            conn.execute("INSERT INTO sessions VALUES (?, ?, strftime('%s','now'), ?)",
                         ('peer:keep', 'user', json.dumps(runs)))
            conn.commit()
            conn.close()

            real_connect = sqlite3.connect
            statements: list[str] = []

            def traced_connect(*args, **kwargs):
                connection = real_connect(*args, **kwargs)
                connection.set_trace_callback(statements.append)
                return connection

            with patch.object(sr.sqlite3, 'connect', traced_connect):
                sr.prune(path, 3, 5, 100, max_bytes=500)

            conn = sqlite3.connect(path)
            self.assertEqual(json.loads(conn.execute('SELECT runs FROM sessions').fetchone()[0]),
                             [{'text': 'newest'}])
            conn.close()
            self.assertFalse([s for s in statements if s.strip().upper().startswith('VACUUM')], statements)

    def test_retention_keeps_running_and_queued_transcripts(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / 'db')
            conn = sqlite3.connect(path)
            conn.execute('CREATE TABLE sessions(session_id TEXT PRIMARY KEY, session_type TEXT, '
                         'updated_at INTEGER, runs TEXT)')
            conn.execute('CREATE TABLE task_runs(session_id TEXT, status TEXT)')
            raw = json.dumps([{'text': 'x' * 600000}])
            for session_id, status in [('scheduler:running', 'running'), ('scheduler:queued', 'queued'),
                                       ('scheduler:finished', 'success')]:
                conn.execute('INSERT INTO sessions VALUES (?,?,0,?)', (session_id, 'agent', raw))
                conn.execute('INSERT INTO task_runs VALUES (?,?)', (session_id, status))
            conn.commit()
            conn.close()

            sr.prune(path, 3, 1, 100, max_bytes=100)

            conn = sqlite3.connect(path)
            self.assertEqual(dict(conn.execute('SELECT session_id, runs FROM sessions')),
                             {'scheduler:running': raw, 'scheduler:queued': raw})
            conn.close()

    def test_vacuum_is_explicit_maintenance_only(self):
        self.assertIn('max_bytes', sr.DEFAULTS)
        with patch('sys.argv', ['session_retention.py', '--daemon', '--vacuum']):
            with self.assertRaises(SystemExit):
                sr.main()


class ConsumedOutboxDeletes(unittest.TestCase):
    def database(self):
        conn = sqlite3.connect(':memory:')
        conn.executescript(OUTBOX_SCHEMA)
        return conn

    def remaining(self, conn):
        return [tuple(row) for row in conn.execute('SELECT source_id, operation FROM search_outbox ORDER BY seq')]

    def test_only_consumed_deletes_lose_their_history(self):
        conn = self.database()
        add_outbox(conn, [('message', 'gone', 'upsert'), ('message', 'gone', 'upsert'), ('message', 'gone', 'delete'),
                          ('message', 'live', 'upsert'),
                          ('session', 'revived', 'delete'), ('session', 'revived', 'upsert'),
                          ('tool_invocation', 'gone2', 'delete')])  # seq 7
        add_outbox(conn, [('tool_invocation', 'unconsumed', 'delete'),  # seq 8 > checkpoint
                          ('message', 'live', 'upsert')])               # seq 9
        conn.execute("INSERT INTO search_outbox_consumers VALUES ('operational-fts-v1', 7)")
        conn.commit()
        # batch_size=1 still takes whole sources: 'gone' never keeps an orphan upsert.
        self.assertEqual(sr.prune_consumed_outbox_deletes(conn, batch_size=1, pause=0), 4)
        self.assertEqual(self.remaining(conn), [('live', 'upsert'), ('revived', 'delete'), ('revived', 'upsert'),
                                                ('unconsumed', 'delete'), ('live', 'upsert')])
        self.assertEqual(sr.prune_consumed_outbox_deletes(conn, pause=0), 0)

    def test_head_row_and_slowest_consumer_are_kept(self):
        conn = self.database()
        add_outbox(conn, [('message', 'a', 'delete'), ('message', 'b', 'delete'), ('message', 'c', 'delete')])
        conn.executemany('INSERT INTO search_outbox_consumers VALUES (?,?)', [('fast', 3), ('slow', 1)])
        conn.commit()
        self.assertEqual(sr.prune_consumed_outbox_deletes(conn, pause=0), 1)
        conn.execute("UPDATE search_outbox_consumers SET last_seq=3 WHERE consumer_id='slow'")
        self.assertEqual(sr.prune_consumed_outbox_deletes(conn, pause=0), 1)
        # Deleting the head would lower MAX(seq) below last_indexed_seq and force a rebuild.
        self.assertEqual(self.remaining(conn), [('c', 'delete')])
        self.assertEqual(conn.execute('SELECT MAX(seq) FROM search_outbox').fetchone()[0], 3)

    def test_nothing_is_consumed_without_a_consumer(self):
        conn = self.database()
        add_outbox(conn, [('message', 'a', 'delete'), ('message', 'b', 'upsert')])
        self.assertEqual(sr.prune_consumed_outbox_deletes(conn, pause=0), 0)
        self.assertEqual(sr.prune_consumed_outbox_deletes(sqlite3.connect(':memory:'), pause=0), 0)


class DeletedRunPayloads(unittest.TestCase):
    def test_tombstoned_terminal_runs_keep_identity_and_lose_payloads(self):
        conn = sqlite3.connect(':memory:')
        conn.execute('PRAGMA foreign_keys=ON')
        conn.executescript(RUNS_SCHEMA)
        heavy = (json.dumps({'p': 'x' * 2000}),) * 5
        for session_id, deleted in [('deleted', 1), ('active', None), ('restored', 1), ('mid-run', 1),
                                    ('queued-task', 1)]:
            conn.execute('INSERT INTO sessions_v2 VALUES (?,?)', (session_id, deleted))
            for index in range(3):
                status = 'running' if session_id == 'mid-run' and index == 2 else 'success'
                conn.execute(
                    'INSERT INTO session_runs(id,session_id,status,input_json,output_json,metrics_json,'
                    'raw_envelope_json,legacy_raw_json,metadata_json) VALUES (?,?,?,?,?,?,?,?,?)',
                    (f'{session_id}-{index}', session_id, status, *heavy, '{"parent":"lineage"}'),
                )
        conn.execute("INSERT INTO sessions(session_id) VALUES ('restored')")
        conn.execute("INSERT INTO task_runs VALUES ('queued-task','queued')")
        conn.commit()

        self.assertEqual(sr.prune_deleted_run_payloads(conn, batch_size=2, pause=0), 3)

        rows = {row[0]: row[1:] for row in conn.execute(
            'SELECT id, status, input_json, output_json, metrics_json, raw_envelope_json, legacy_raw_json, '
            'metadata_json FROM session_runs')}
        self.assertEqual(len(rows), 15)
        for index in range(3):
            self.assertEqual(rows[f'deleted-{index}'],
                             ('success', None, None, None, '{}', None, '{"parent":"lineage"}'))
        for session_id in ('active', 'restored', 'mid-run', 'queued-task'):
            for index in range(3):
                self.assertEqual(rows[f'{session_id}-{index}'][1], heavy[0], session_id)
        self.assertEqual(conn.execute('PRAGMA foreign_key_check').fetchall(), [])
        self.assertEqual(sr.prune_deleted_run_payloads(conn, pause=0), 0)

    def test_legacy_database_remains_supported(self):
        conn = sqlite3.connect(':memory:')
        conn.execute('CREATE TABLE sessions(session_id TEXT PRIMARY KEY)')
        self.assertEqual(sr.prune_deleted_run_payloads(conn, pause=0), 0)

    def test_prune_runs_both_reclaimers_on_a_file_db(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / 'db')
            conn = sqlite3.connect(path)
            conn.executescript(RUNS_SCHEMA + OUTBOX_SCHEMA)
            conn.execute("INSERT INTO sessions_v2 VALUES ('gone', 1)")
            conn.execute("INSERT INTO session_runs(id,session_id,status,input_json,raw_envelope_json) "
                         "VALUES ('r','gone','failed','[1]','{\"a\":1}')")
            add_outbox(conn, [('session', 'gone', 'delete'), ('session', 'other', 'upsert')])
            conn.execute("INSERT INTO search_outbox_consumers VALUES ('operational-fts-v1', 2)")
            conn.commit()
            conn.close()

            sr.prune(path, 3, 5, 100)

            conn = sqlite3.connect(path)
            self.assertEqual(conn.execute('SELECT input_json, raw_envelope_json FROM session_runs').fetchone(),
                             (None, '{}'))
            self.assertEqual(conn.execute('SELECT source_id FROM search_outbox').fetchall(), [('other',)])
            conn.close()


if __name__ == '__main__':
    unittest.main()
