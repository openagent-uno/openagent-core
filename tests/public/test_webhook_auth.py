import hashlib
import hmac
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from openagent_core.webhook_auth import authenticate, extract_external_id, WebhookAuthError
from openagent_core.core.event_secret import make_secret_material


class WebhookAuthTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "state.sqlite")
        self.secret, self.encrypted, _ = make_secret_material(db_path=self.db_path)
        self.raw = b'{ "id" : "event-1", "message" : "original bytes" }'

    def tearDown(self):
        self.tmp.cleanup()

    def verify(self, kind, headers, raw=None):
        authenticate(event={"type": kind, "secret_enc": self.encrypted},
            raw_body=self.raw if raw is None else raw, headers=headers, db_path=self.db_path)

    def sign(self, raw):
        return hmac.new(self.secret.encode(), raw, hashlib.sha256).hexdigest()

    def test_hmac_providers_require_the_original_bytes_and_reject_bad_optional_secret(self):
        for kind, name in (("github", "X-Hub-Signature-256"), ("generic-hmac", "X-Signature-256")):
            headers = {name: "sha256=" + self.sign(self.raw)}
            self.verify(kind, headers)
            with self.assertRaises(WebhookAuthError):
                self.verify(kind, headers, self.raw + b" ")
            with self.assertRaises(WebhookAuthError):
                self.verify(kind, {**headers, "Authorization": "Bearer wrong"})
            with self.assertRaises(WebhookAuthError):
                self.verify(kind, {"Authorization": "Bearer " + self.secret})

    def test_generic_secret_and_provider_delivery_id_contract(self):
        self.verify("generic", {"Authorization": "Bearer " + self.secret})
        self.verify("generic", {"X-OpenAgent-Event-Secret": self.secret})
        with self.assertRaises(WebhookAuthError):
            self.verify("generic", {})
        self.assertEqual("github-id", extract_external_id(event={"type": "github"},
            headers={"X-GitHub-Delivery": "github-id"}, payload={}))
        self.assertEqual("stripe-id", extract_external_id(event={"type": "stripe"},
            headers={}, payload={"id": "stripe-id"}))

    def test_timestamped_schemes_reject_stale_future_and_modified_payloads(self):
        now = int(time.time())
        for kind in ("stripe", "slack"):
            for offset in (0, -600, 600):
                ts = str(now + offset)
                if kind == "stripe":
                    headers = {"Stripe-Signature": "t=" + ts + ",v1=" + self.sign(ts.encode() + b"." + self.raw)}
                else:
                    headers = {"X-Slack-Request-Timestamp": ts,
                        "X-Slack-Signature": "v0=" + self.sign(b"v0:" + ts.encode() + b":" + self.raw)}
                if offset:
                    with self.assertRaises(WebhookAuthError):
                        self.verify(kind, headers)
                else:
                    self.verify(kind, headers)
                    with self.assertRaises(WebhookAuthError):
                        self.verify(kind, headers, self.raw + b" ")

    def test_import_has_no_engine_provider_crypto_or_storage_side_effects(self):
        result = subprocess.run([sys.executable, "-c",
            "import sys; import openagent_core.webhook_auth; "
            "assert not any(k.startswith(('openagent_core.providers', 'agno', 'cryptography', 'aiosqlite')) for k in sys.modules)"],
            capture_output=True, text=True)
        self.assertEqual(0, result.returncode, result.stderr)
