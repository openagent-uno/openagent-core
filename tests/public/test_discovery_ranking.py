from types import SimpleNamespace
import unittest

from openagent_core.mcp.servers.tool_search.ranking import rank_tools


def tool(name, description="", *, source="source", properties=None):
    return SimpleNamespace(name=name, description=description, source_id=source,
                           target_label=source, input_schema={"properties": properties or {}})


class DiscoveryRanking(unittest.TestCase):
    def test_exact_name_and_phrase_beat_generic_description(self):
        exact = tool("create_adset")
        prefixed = tool("ads_create_adset")
        generic = tool("generic", "Create adset. " * 100)
        self.assertEqual(rank_tools([generic, prefixed, exact], "create adset"),
                         [exact, prefixed, generic])

    def test_rare_terms_outweigh_frequent_catalog_boilerplate(self):
        common = [tool(f"operation_{index}", "Manage account settings") for index in range(30)]
        rare = tool("certificate", "Rotate signing certificate")
        ranked = rank_tools([*common, rare], "account certificate")
        self.assertIs(ranked[0], rare)

    def test_argument_names_are_searchable_but_schema_prose_is_not(self):
        selected = tool("lookup", properties={"invoiceId": {"description": "hidden prose secretword"}})
        self.assertEqual(rank_tools([selected], "invoice"), [selected])
        self.assertEqual(rank_tools([selected], "secretword"), [])
        self.assertEqual(rank_tools([selected], "look"), [])  # No substring false positives.

    def test_unicode_camel_case_empty_and_deterministic_source_ties(self):
        first, second = tool("readRésumé", source="a"), tool("readRésumé", source="b")
        self.assertEqual(rank_tools([second, first], "RÉSUMÉ"), [first, second])
        self.assertEqual(rank_tools([second, first], ""), [first, second])
        self.assertEqual(rank_tools([first], "___"), [])
        self.assertEqual(rank_tools([], "read"), [])
