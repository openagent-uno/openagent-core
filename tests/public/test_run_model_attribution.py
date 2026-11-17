"""A run records which model answered, so history shows it after a reload."""
import json
import sqlite3
import unittest

from openagent_core.memory.operational.repository import answering_model, apply_run_envelope


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

    def test_a_backup_that_answered_is_recorded_instead_of_the_model_the_run_started_with(self):
        envelope = {"run_id": "run-1", "model": "primary-llm", "model_provider": "Primary",
                    "metrics": {"details": {"model": [
                        {"id": "backup-llm", "provider": "Backup", "output_tokens": 40}]}}}
        self.assertEqual(("backup-llm", "Backup"), answering_model(envelope))
        apply_run_envelope(self.conn, "s", envelope)
        self.assertEqual(("backup-llm", "Backup", 1), self._row("run-1"))

    def test_the_model_that_wrote_the_most_output_answered(self):
        used = [{"id": "primary-llm", "provider": "Primary Chat", "output_tokens": 5},
                {"id": "backup-llm", "provider": "Backup", "output_tokens": 90}]
        envelope = {"model": "primary-llm", "model_provider": "Primary", "metrics": {"details": {"model": used}}}
        self.assertEqual(("backup-llm", "Backup"), answering_model(envelope))
        # The run's own model keeps the provider label the envelope gives it.
        used[0]["output_tokens"] = 900
        self.assertEqual(("primary-llm", "Primary"), answering_model(envelope))

    def test_an_envelope_without_usable_usage_names_its_own_model(self):
        for metrics in (None, {}, {"details": None}, {"details": {"model": []}},
                        {"details": {"model": [{"id": " ", "output_tokens": 3}, "x"]}},
                        {"details": {"output_model": [{"id": "parser", "output_tokens": 3}]}}):
            self.assertEqual(("fixture-llm", "Fixture"), answering_model(
                {"model": "fixture-llm", "model_provider": "Fixture", "metrics": metrics}))
        self.assertEqual((None, None), answering_model(None))
        self.assertEqual((None, None), answering_model({"model": 7}))


if __name__ == "__main__":
    unittest.main()
