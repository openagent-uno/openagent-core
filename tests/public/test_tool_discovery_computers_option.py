"""A host without computer tools can leave the computers guidance out of the prompt."""
import asyncio
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "packages" / "module-tool-discovery" / "src"))

from openagent_core.modules import ModuleConfig  # noqa: E402
from openagent_module_tool_discovery import descriptor  # noqa: E402


def _blocks(options):
    config = ModuleConfig(surfaces={"service", "agent_tools"}, options=options)
    descriptor.validate(config)
    agent = SimpleNamespace(capability_pool=SimpleNamespace(specs=(SimpleNamespace(name="tool-search"),)))
    services = SimpleNamespace(require=lambda key: SimpleNamespace(agent=agent))
    context = SimpleNamespace(config=config, services=services)

    async def start():
        module = await descriptor.prepare(context)
        return (await module.start()).prompt_blocks

    return "\n".join(str(getattr(block, "text", block)) for block in asyncio.run(start()))


class ComputersGuidanceOption(unittest.TestCase):
    def test_the_guidance_is_included_by_default(self):
        self.assertIn("Computers and browsers", _blocks({}))
        self.assertIn("Computers and browsers", _blocks({"computers_guidance": True}))

    def test_a_host_can_turn_it_off_and_keeps_the_rest(self):
        text = _blocks({"computers_guidance": False})
        self.assertNotIn("Computers and browsers", text)
        self.assertTrue(text.strip())

    def test_a_wrong_or_unknown_option_is_refused(self):
        for options in ({"computers_guidance": "no"}, {"computers": False}):
            with self.assertRaises(ValueError):
                descriptor.validate(ModuleConfig(surfaces={"service", "agent_tools"}, options=options))


if __name__ == "__main__":
    unittest.main()
