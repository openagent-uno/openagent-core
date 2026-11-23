"""Exercise the same durable-operation suite against an independent PostgreSQL store."""

import os
import unittest
from uuid import uuid4

# Import the module so pytest collects exactly one suite from this file.
import test_durable_operations


@unittest.skipUnless(
    os.environ.get("OPENAGENT_TEST_POSTGRES_DSN"), "requires disposable PostgreSQL"
)
class PostgresDurableOperations(test_durable_operations.DurableOperationsTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.schema = "oa_durable_" + uuid4().hex

    def make_store(self):
        from openagent_storage_postgres import PostgresRuntimeStore

        return PostgresRuntimeStore(
            os.environ["OPENAGENT_TEST_POSTGRES_DSN"], agent_id="agent-a", schema=self.schema
        )
