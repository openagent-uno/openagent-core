"""The qualification runner never installs unverified or ambiguous wheel bytes."""
from hashlib import sha256
import importlib.util
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
import zipfile


ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("core_qualification", ROOT / "packaging/qualify.py")
qualification = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qualification)


class QualificationCohort(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name)
        self.wheel = self.path / "fixture-1.0-py3-none-any.whl"
        with zipfile.ZipFile(self.wheel, "w") as archive:
            archive.writestr("fixture-1.0.dist-info/METADATA", "Name: fixture\nVersion: 1.0\n")
        self.manifest = {"format": 1, "dirty_snapshot": False, "wheels": [{
            "name": "fixture", "version": "1.0", "file": self.wheel.name,
            "sha256": sha256(self.wheel.read_bytes()).hexdigest()}]}
        self.write()

    def write(self):
        (self.path / "manifest.json").write_text(json.dumps(self.manifest))

    def test_exact_cohort_is_accepted(self):
        manifest, paths = qualification.candidate_wheels(self.path)
        self.assertEqual(manifest, self.manifest)
        self.assertEqual(paths, [self.wheel])

    def test_dirty_source_needs_explicit_development_opt_in(self):
        self.manifest["dirty_snapshot"] = True
        self.write()
        with self.assertRaises(ValueError):
            qualification.candidate_wheels(self.path)
        self.assertEqual(qualification.candidate_wheels(self.path, allow_dirty=True)[1], [self.wheel])

    def test_tampered_wheel_cannot_be_installed(self):
        self.wheel.write_bytes(b"different bytes")
        with self.assertRaisesRegex(ValueError, "digest mismatch"):
            qualification.candidate_wheels(self.path)

    def test_metadata_mismatch_duplicate_distribution_and_path_escape_fail(self):
        for change in ({"name": "another"}, {"version": "2.0"}, {"file": "../escape.whl"}):
            original = dict(self.manifest["wheels"][0])
            self.manifest["wheels"][0].update(change)
            self.write()
            with self.subTest(change=change), self.assertRaises(ValueError):
                qualification.candidate_wheels(self.path)
            self.manifest["wheels"][0] = original
        self.manifest["wheels"] *= 2
        self.write()
        with self.assertRaisesRegex(ValueError, "metadata"):
            qualification.candidate_wheels(self.path)

    def test_empty_or_incomplete_manifest_is_not_qualification(self):
        for value in ({}, {"format": 1, "wheels": []}, {"format": 1, "dirty_snapshot": False, "wheels": []}):
            (self.path / "manifest.json").write_text(json.dumps(value))
            with self.subTest(value=value), self.assertRaises(ValueError):
                qualification.candidate_wheels(self.path)
