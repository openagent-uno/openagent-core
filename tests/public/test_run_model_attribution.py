"""A run records which model answered, so history shows it after a reload."""
import json
import sqlite3
import unittest

from openagent_core.memory.operational.repository import apply_run_envelope


class RunModelAttribution(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("CREATE TABLE session_runs (id TEXT, session_id TEXT, metadata_json TEXT, "
                          "raw_envelope_json TEXT, model TEXT, model_provider TEXT)")
        self.conn.executemany("INSERT INTO session_runs VALUES (?,?,?,?,?,?)", [
            ("run-1", "s", json.dumps({"runtime_contract": 1}), None, None, None),
            ("run-2", "s", json.dumps({"runtime_contract": 1}), None, "pinned-model", None),
            ("legacy", "s", "{}", None, None, None),
        ])

    def _row(self, run_id):
        return self.conn.execute("SELECT model, model_provider, raw_envelope_json IS NOT NULL "
                                 "FROM session_runs WHERE id=?", (run_id,)).fetchone()

    def test_the_answering_model_is_recorded_with_the_envelope(self):
        apply_run_envelope(self.conn, "s", {"run_id": "run-1", "model": "fixture-llm", "model_provider": "Fixture"})
        self.assertEqual(("fixture-llm", "Fixture", 1), self._row("run-1"))

    def test_a_recorded_model_is_kept(self):
        apply_run_envelope(self.conn, "s", {"run_id": "run-2", "model": "other"})
        self.assertEqual("pinned-model", self._row("run-2")[0])

    def test_an_envelope_without_a_model_or_for_another_run_changes_nothing_else(self):
        apply_run_envelope(self.conn, "s", {"run_id": "run-1", "model": "  "})
        self.assertEqual((None, None, 1), self._row("run-1"))
        apply_run_envelope(self.conn, "s", {"run_id": "legacy", "model": "fixture-llm"})
        apply_run_envelope(self.conn, "other-session", {"run_id": "run-2", "model": "x"})
        self.assertEqual((None, None, 0), self._row("legacy"))
        self.assertEqual(0, self._row("run-2")[2])


if __name__ == "__main__":
    unittest.main()
