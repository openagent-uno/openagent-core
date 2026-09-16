"""Public composition adapters remain importable without private namespaces."""
import unittest

class EngineExports(unittest.TestCase):
    def test_pool_source_is_a_public_engine_adapter(self):
        from openagent_core.engine import MCPPool, PoolCapabilitySource
        self.assertTrue(callable(MCPPool.from_config))
        self.assertTrue(callable(PoolCapabilitySource.discover))
        self.assertTrue(callable(PoolCapabilitySource.call_tool))
