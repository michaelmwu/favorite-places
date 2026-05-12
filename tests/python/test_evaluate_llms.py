from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts import evaluate_llms
from scripts.pipeline_models import EnrichmentPlace, RawPlace


class EvaluateLLMsTest(unittest.TestCase):
    def test_resolve_model_profiles_supports_openrouter_prefix(self) -> None:
        profiles = evaluate_llms.resolve_model_profiles(
            "openrouter:anthropic/claude-sonnet-4.5",
            profiles_path=None,
            fallback_base_url=None,
        )

        self.assertEqual(len(profiles), 1)
        self.assertEqual(profiles[0].api_key_env, "OPENROUTER_API_KEY")
        self.assertEqual(profiles[0].base_url, "https://openrouter.ai/api/v1")
        self.assertEqual(profiles[0].model, "anthropic/claude-sonnet-4.5")

    def test_resolve_model_profiles_loads_default_json_profiles(self) -> None:
        profiles = evaluate_llms.resolve_model_profiles(
            "gpt-5.4-mini,kimi-k2p6-fireworks",
            profiles_path=None,
            fallback_base_url=None,
        )

        self.assertEqual([profile.name for profile in profiles], ["gpt-5.4-mini", "kimi-k2p6-fireworks"])
        self.assertEqual(profiles[0].cost_per_1m["input"], 0.75)
        self.assertEqual(profiles[1].api_key_env, "FIREWORKS_API_KEY")

    def test_load_profile_overrides_supports_custom_provider(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "profiles.json"
            path.write_text(
                json.dumps(
                    {
                        "models": {
                            "custom-fireworks": {
                                "model": "accounts/fireworks/models/example",
                                "api_key_env": "FIREWORKS_API_KEY",
                                "base_url": "https://api.fireworks.ai/inference/v1",
                                "request_options": {"response_format": {"type": "json_object"}},
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )

            profiles = evaluate_llms.resolve_model_profiles(
                "custom-fireworks",
                profiles_path=path,
                fallback_base_url=None,
            )

        self.assertEqual(profiles[0].api_key_env, "FIREWORKS_API_KEY")
        self.assertEqual(profiles[0].model, "accounts/fireworks/models/example")

    def test_write_semantic_judge_pack_prioritizes_baseline_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "judge-pack.md"
            evaluate_llms.write_judge_pack(
                path,
                task="semantic",
                baseline="gpt-5.5",
                records=[
                    {
                        "case_id": "tokyo:cid:1",
                        "place_name": "Tea House",
                        "guide_slug": "tokyo",
                        "evidence": {"name": "Tea House", "category": "Cafe"},
                        "current_semantic": {"description": None},
                        "outputs": {
                            "kimi-k2p6-turbo-fireworks": {"accepted": {"description": "Candidate"}},
                            "gpt-5.5": {"accepted": {"description": None}},
                        },
                    }
                ],
            )
            content = path.read_text(encoding="utf-8")

        self.assertIn("LLM semantic Judge Pack", content)
        self.assertLess(content.index("### gpt-5.5"), content.index("### kimi-k2p6-turbo-fireworks"))

    def test_estimated_response_cost_uses_cached_input_pricing(self) -> None:
        profile = evaluate_llms.ModelProfile(
            name="priced",
            provider="openai-compatible",
            model="priced",
            api_key_env="OPENAI_API_KEY",
            cost_per_1m={"input": 2.0, "cached": 0.2, "output": 10.0},
        )

        cost = evaluate_llms.estimated_response_cost_usd(
            profile,
            {
                "prompt_tokens": 1000,
                "completion_tokens": 100,
                "prompt_tokens_details": {"cached_tokens": 400},
            },
        )

        self.assertEqual(cost, 0.00228)

    def test_semantic_case_labels_mark_sparse_and_non_english(self) -> None:
        labels = evaluate_llms.semantic_case_labels(
            raw_place=RawPlace(
                name="紅鱻客家小館",
                maps_url="https://maps.example/place",
            ),
            enrichment=EnrichmentPlace(primary_type_display_name="Hakka restaurant"),
            city_name="Taoyuan",
            country_name="Taiwan",
        )

        self.assertIn("evidence:sparse", labels)
        self.assertIn("name:non-english", labels)
        self.assertIn("type:food", labels)
        self.assertIn("geo:asia", labels)

    def test_select_stratified_semantic_cases_prefers_coverage(self) -> None:
        cases = [
            {"case_id": "a", "fixture_labels": ["evidence:sparse", "geo:asia"]},
            {"case_id": "b", "fixture_labels": ["evidence:raw-note", "geo:europe"]},
            {"case_id": "c", "fixture_labels": ["evidence:review-topics", "type:food"]},
        ]

        selected = evaluate_llms.select_stratified_semantic_cases(cases, limit=2)

        self.assertEqual(len(selected), 2)
        selected_labels = {label for case in selected for label in case["fixture_labels"]}
        self.assertTrue({"evidence:sparse", "evidence:raw-note"} <= selected_labels)


if __name__ == "__main__":
    unittest.main()
