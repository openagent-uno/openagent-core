"""Real CAS bytes and durable session links with a changing host authorizer."""
import asyncio
from dataclasses import replace
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from openagent_core import Runtime, RuntimeServices, RuntimeSettings, RunRequest
from openagent_core.artifacts import ArtifactRepository
from openagent_core.administration import ManagementContext
from openagent_core.contracts import PrincipalRef, ExecutionContext
from openagent_core.memory.artifacts import ArtifactIntegrityError
from openagent_storage_sqlite import SqliteRuntimeStore


class Policy:
    def __init__(self):
        self.grants = set()
        self.revoked = False

    async def authorize(self, context, action, resource, *, audience=()):
        if self.revoked:
            return False
        if not action.startswith("artifact."):
            return True
        if resource.kind == "agent":
            return resource.resource_id == context.agent_id
        if resource.kind == "principal":
            return resource.resource_id == context.authority.key and not audience
        if resource.kind == "session":
            readers = tuple(audience) or (context.authority,)
            return all((p.subject_id, resource.resource_id) in self.grants for p in readers)
        return False


class Executor:
    async def execute(self, request, context, runtime):
        return "fixture"


class Artifacts(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.policy = Policy()
        self.a = PrincipalRef("host", "tenant", "alice")
        self.b = PrincipalRef("host", "tenant", "bob")
        self.alice = ManagementContext(self.a, "agent")
        self.bob = ManagementContext(self.b, "agent")
        self.store = SqliteRuntimeStore(self.root / "state.db")
        self.runtime = Runtime(RuntimeSettings("agent", self.root), RuntimeServices(self.store, Executor(), self.policy))
        await self.runtime.start()
        self.repo = ArtifactRepository(SimpleNamespace(db_path=str(self.store.path)), self.policy)

    async def asyncTearDown(self):
        await self.runtime.close()
        self.temp.cleanup()

    async def session(self, name):
        context = ExecutionContext(self.a, self.a, self.a, name, "agent", (self.a,))
        await self.runtime.submit(RunRequest("run-" + name, name, "key-" + name, "fixture"), context)
        await self.runtime.wait("run-" + name, context)

    async def test_private_owner_exact_tenant_and_legacy_path(self):
        result = await self.repo.upload(self.alice, b"private", filename="private.txt")
        self.assertEqual((await self.repo.read(self.alice, artifact_id=result["artifact_id"])).content, b"private")
        self.assertEqual((await self.repo.read(self.alice, legacy_path=result["path"])).content, b"private")
        for context in (self.bob, ManagementContext(PrincipalRef("host", "other", "alice"), "agent")):
            with self.assertRaises(LookupError):
                await self.repo.read(context, artifact_id=result["artifact_id"])
        outside = self.root / "secret.txt"
        outside.write_text("not an artifact")
        with self.assertRaises(LookupError):
            await self.repo.read(self.alice, legacy_path=str(outside))

    async def test_dedup_uses_authorized_link_filename_and_live_grants(self):
        first = await self.repo.upload(self.alice, b"identical", filename="private-project-name.txt")
        await self.session("shared")
        self.policy.grants.add(("bob", "shared"))
        second = await self.repo.upload(self.bob, b"identical", filename="public-name.txt", session_id="shared")
        self.assertEqual(first["artifact_id"], second["artifact_id"])
        value = await self.repo.read(self.bob, artifact_id=first["artifact_id"])
        self.assertEqual(value.filename, "public-name.txt")
        self.policy.grants.clear()
        with self.assertRaises(LookupError):
            await self.repo.read(self.bob, artifact_id=first["artifact_id"])

    async def test_shared_audience_cannot_inherit_private_owner_access(self):
        artifact = await self.repo.upload(self.alice, b"private", filename="private.txt")
        shared = ExecutionContext(self.a, self.a, self.a, "shared", "agent", (self.a, self.b))
        with self.assertRaises(LookupError):
            await self.repo.read(shared, artifact_id=artifact["artifact_id"])
        await self.session("shared")
        self.policy.grants.update({("alice", "shared"), ("bob", "shared")})
        await self.repo.upload(self.alice, b"private", filename="shared-name.txt", session_id="shared")
        self.assertEqual((await self.repo.read(shared, artifact_id=artifact["artifact_id"])).filename, "shared-name.txt")
        self.policy.grants.remove(("bob", "shared"))
        with self.assertRaises(LookupError):
            await self.repo.read(shared, artifact_id=artifact["artifact_id"])

    async def test_revocation_during_io_prevents_delivery(self):
        artifact = await self.repo.upload(self.alice, b"private", filename="private.txt")
        read = self.repo._bytes
        def revoke(row):
            value = read(row)
            self.policy.revoked = True
            return value
        self.repo._bytes = revoke
        with self.assertRaises(PermissionError):
            await self.repo.read(self.alice, artifact_id=artifact["artifact_id"])

    async def test_corruption_links_and_bounded_upload(self):
        artifact = await self.repo.upload(self.alice, b"original", filename="a.txt")
        path = Path(artifact["path"])
        path.write_bytes(b"tampered")
        with self.assertRaises(ArtifactIntegrityError):
            await self.repo.read(self.alice, artifact_id=artifact["artifact_id"])
        path.unlink()
        original = self.root / "original"
        original.write_bytes(b"original")
        path.symlink_to(original)
        with self.assertRaises(ArtifactIntegrityError):
            await self.repo.read(self.alice, artifact_id=artifact["artifact_id"])
        self.repo.max_input_bytes = 1
        with self.assertRaises(ValueError):
            await self.repo.upload(self.alice, b"too large", filename="big")


if __name__ == "__main__":
    unittest.main()
