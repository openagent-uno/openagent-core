"""Guards on the text of every shipped prompt block: names, size and neutrality.

The framework and module blocks are sent on every run, so a stale source name,
an essay that creeps back in or a product-specific reference costs every turn.
These tests read the real shipped blocks (``prompts/rules.json`` plus the
blocks module packages contribute), never a hand-kept copy.
"""
from __future__ import annotations

import importlib
import json
from pathlib import Path
import re
import unittest

from openagent_core.prompts import (
    PromptComposer, SECTION_ORDER, core_framework_blocks, rule_blocks,
)

ROOT = Path(__file__).resolve().parents[2]
RULES = ROOT / "packages/runtime/src/openagent_core/prompts/rules.json"

# Module packages importable in the core test environment that contribute
# prompt blocks of their own (beyond rule ids in rules.json).
_MODULES = ("events", "mcp", "scheduler", "search", "sessions", "tool-discovery", "vault", "workflows")

# Budget for the composed framework + every module block, in characters
# (about 4 characters per token). The rewrite brought it from about 33.8k to
# about 14.9k; raise it only together with a reviewed, deliberate addition.
TOTAL_BUDGET_CHARS = 18_000
BLOCK_BUDGET_CHARS = 2_400
# The tools section grew to 4,800 in 1.1.0b110 for `core.computers` (how to
# choose between shell, the agent's browser and screen control on a target).
SECTION_BUDGET_CHARS = {"memory": 4_800, "tools": 4_800}

# A backticked, lowercase, hyphenated word with no underscore names a source
# (`vault`, `vault-gate`, `schedules`); tool names and parameters carry an
# underscore and are checked by the legacy prompt-name suite.
_SOURCE_TOKEN = re.compile(r"`{1,2}([a-z][a-z0-9]*(?:-[a-z0-9]+)*)`{1,2}")


def shipped_rule_ids() -> tuple[str, ...]:
    return tuple(entry["id"] for entry in json.loads(RULES.read_text()))


def module_blocks() -> list:
    blocks = []
    for name in _MODULES:
        module = importlib.import_module("openagent_module_" + name.replace("-", "_"))
        descriptor = getattr(module, "descriptor", None)
        blocks.extend(rule_blocks(*getattr(descriptor, "prompt_rule_ids", ())))
        blocks.extend(getattr(descriptor, "additional_prompt_blocks", ()))
        for constant in ("SESSION_PROMPT", "SEARCH_PROMPT", "MCP_PROMPT"):
            if hasattr(module, constant):
                blocks.append(getattr(module, constant))
    return blocks


def all_blocks() -> list:
    """Every shipped block exactly once: all rules plus module-owned blocks."""
    blocks = {block.id: block for block in rule_blocks(*shipped_rule_ids())}
    for block in module_blocks():
        blocks.setdefault(block.id, block)
    return list(blocks.values())


def known_sources() -> tuple[set[str], set[str]]:
    """(current source ids, compatibility aliases) from the code that registers them."""
    from openagent_core.capability_map import SOURCE_ALIASES, SOURCE_SUMMARIES
    current = set(SOURCE_SUMMARIES)
    for name in _MODULES:
        descriptor = getattr(importlib.import_module(
            "openagent_module_" + name.replace("-", "_")), "descriptor", None)
        current.update(getattr(descriptor, "source_names", {}).values())
    # Sources registered by module code rather than a descriptor table.
    current.update({"sessions", "search", "tool-search"})
    aliases = {alias for alias in SOURCE_ALIASES if alias not in current}
    return current, aliases


class PromptLintTests(unittest.TestCase):
    def test_blocks_name_only_current_sources(self):
        current, aliases = known_sources()
        named: set[str] = set()
        for block in all_blocks():
            for token in _SOURCE_TOKEN.findall(block.text):
                named.add(token)
                self.assertIn(token, current | aliases,
                              f"{block.id} names unknown source `{token}`")
                # Aliases keep old habits working in discovery, but shipped
                # prompts must teach the current name (F3: `scheduler` and
                # `workflow-manager` used to find nothing).
                self.assertNotIn(token, aliases,
                                 f"{block.id} names the old source `{token}`; use its current id")
        # Not vacuous: the prompts do name the main sources.
        self.assertTrue({"vault", "vault-gate", "schedules", "workflows", "events",
                         "delegation"} <= named, named)

    def test_composed_framework_and_modules_fit_the_budget(self):
        blocks = all_blocks()
        always_on = {block.id for block in core_framework_blocks()}
        composed = PromptComposer().compose(
            modules=[block for block in blocks if block.id not in always_on],
            substitutions={"OPENAGENT_VAULT_PATH": "/vault"}).stable
        self.assertLessEqual(len(composed), TOTAL_BUDGET_CHARS,
                             f"framework + modules is {len(composed)} characters")
        sections: dict[str, int] = {}
        for block in blocks:
            self.assertLessEqual(len(block.text), BLOCK_BUDGET_CHARS, block.id)
            sections[block.section] = sections.get(block.section, 0) + len(block.text)
        for section, budget in SECTION_BUDGET_CHARS.items():
            self.assertLessEqual(sections.get(section, 0), budget, section)
        self.assertTrue(set(sections) <= set(SECTION_ORDER), sections)
        # Every rule in rules.json is reachable: always on, or selected by a
        # module package (read from every package's source, importable or not).
        selected = set(always_on)
        for source in (ROOT / "packages").glob("module-*/src/*/__init__.py"):
            text = source.read_text()
            for group in re.findall(r"(?:prompt_rule_ids\s*=\s*|rule_blocks\()\(?([^)]*)\)", text):
                selected.update(re.findall(r"[\"']([a-z]+\.[a-z.-]+)[\"']", group))
        self.assertEqual(set(shipped_rule_ids()) - selected, set(), "a rule no module selects")
        self.assertEqual(selected - set(shipped_rule_ids()), set(), "a module selects an unknown rule")

    def test_blocks_are_agnostic_and_current(self):
        forbidden = (
            r"\bClaude\b", r"\bHermes\b", r"vision §", r"\bObsidian\b", r"Company-Brain",
            r"\bTelegram\b", r"\bDiscord\b", r"\bWhatsApp\b", r"\bSlack\b",
            # Another agent's built-in tool names.
            r"`{1,2}(?:Read|Edit|Write|NotebookEdit|Bash|Grep|Glob)`{1,2}",
            # Stale runtime claims.
            r"catalog above", r"\btrimmed\b", r"<session-id>", r"search_past_conversations",
        )
        for block in all_blocks():
            for pattern in forbidden:
                self.assertIsNone(re.search(pattern, block.text),
                                  f"{block.id} matches {pattern!r}")

    def test_computer_rules_keep_their_non_negotiables(self):
        # A live run declared itself blocked because its only data source was
        # read-only, although a signed-in browser on a connected device could
        # do the job; the rule tells the agent to look across targets first.
        text = rule_blocks("core.computers")[0].text
        for rule in ("use the narrowest kind that does the job",
                     "A read-only source is not the only way to act.",
                     "check whether another target in `<capabilities>`",
                     "After each action that changes something, check the result",
                     "Never type passwords, one-time codes or payment details",
                     "never\nsolve a CAPTCHA",
                     "Stop before an irreversible or public action"):
            self.assertIn(rule, text)
        from openagent_module_tool_discovery import ToolDiscoveryModule  # noqa: F401
        source = (ROOT / "packages/module-tool-discovery/src/openagent_module_tool_discovery/__init__.py").read_text()
        self.assertIn('"core.tools","core.computers","core.managers"', source)

    def test_delegation_rules_do_not_contradict(self):
        text = rule_blocks("module.delegation")[0].text
        self.assertIn("The host decides how much to delegate", text)
        self.assertIn("if you are a delegated worker", text)
        self.assertIn("do not delegate it again", text)
        self.assertNotRegex(text, r"(?i)always (?:break|delegate)")

    def test_vault_reminder_uses_the_prompt_tool_names(self):
        from openagent_core.learning.vault_reminder import _REMINDER_TEMPLATE
        memory = "\n".join(block.text for block in all_blocks() if block.section == "memory")
        for tool in re.findall(r"\bvault_[a-z_]+", _REMINDER_TEMPLATE):
            self.assertIn(f"`{tool}`", memory, tool)
        self.assertIn("vault_search ", _REMINDER_TEMPLATE)
        self.assertNotIn("vault_search_notes", _REMINDER_TEMPLATE)


if __name__ == "__main__":
    unittest.main()
