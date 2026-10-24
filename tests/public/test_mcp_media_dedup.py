import base64
from copy import deepcopy
from types import SimpleNamespace
import unittest

from openagent_core.mcp.media_dedup import deduplicate_image_aliases


def receipt():
    data = base64.b64encode(b"synthetic image bytes" * 1000).decode()
    return {"content": [{"type": "text", "text": "synthetic receipt"},
                        {"type": "image", "mimeType": "image/png", "data": data}],
            "images": [{"id": "render-handle", "content": data, "mime_type": "image/png",
                        "alt_text": None, "filepath": None}],
            "isError": False, "_meta": {"receipt": "synthetic"},
            "structuredContent": {"read": True}}


class MediaDedupTests(unittest.TestCase):
    def test_only_duplicate_sdk_alias_is_removed_and_input_is_unchanged(self):
        original = receipt(); before = deepcopy(original)
        compact = deduplicate_image_aliases(original)
        self.assertNotIn("images", compact)
        self.assertEqual(compact["content"], before["content"])
        for key in ("isError", "_meta", "structuredContent"):
            self.assertEqual(compact[key], before[key])
        self.assertEqual(original, before)

    def test_distinct_media_and_meaningful_metadata_survive(self):
        for extra in ({"content": "different"}, {"mime_type": "image/jpeg"},
                      {"alt_text": "meaningful description"}, {"future_metadata": "keep"},
                      {"url": "https://example.invalid/synthetic"}):
            value=receipt(); value["images"][0].update(extra)
            self.assertIs(deduplicate_image_aliases(value), value)
        value=receipt(); extra={"content":"different","mime_type":"image/png"}
        value["images"].append(extra)
        self.assertEqual(deduplicate_image_aliases(value)["images"], [extra])

    def test_runtime_codec_deduplicates_sdk_results_but_preserves_native_json(self):
        from openagent_core.mcp.servers.tool_search import adapters
        value=receipt()
        sdk=SimpleNamespace(mcp_result={k:v for k,v in value.items() if k!="images"}, images=value["images"])
        first=adapters._coerce_to_jsonable
        compact=first(sdk)
        self.assertNotIn("images",compact)
        self.assertEqual(compact["content"],value["content"])
        self.assertEqual(first(value),value)


if __name__ == "__main__":
    unittest.main()
