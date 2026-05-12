from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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

    def test_unknown_model_with_base_url_can_override_api_key_env(self) -> None:
        profiles = evaluate_llms.resolve_model_profiles(
            "custom-model",
            profiles_path=None,
            fallback_base_url="http://localhost:8080/v1",
            fallback_api_key_env="CUSTOM_LLM_API_KEY",
        )

        self.assertEqual(profiles[0].api_key_env, "CUSTOM_LLM_API_KEY")
        self.assertEqual(profiles[0].base_url, "http://localhost:8080/v1")

    def test_task_alias_style_arguments_accept_global_options_after_subcommand(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            cases_file = root / "semantic-cases.jsonl"
            cases_file.write_text("", encoding="utf-8")

            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = evaluate_llms.main(
                    [
                        "semantic",
                        "--models",
                        "gpt-5.4-mini",
                        "--run-name",
                        "alias-semantic",
                        "--output-root",
                        str(root),
                        "--cases-file",
                        str(cases_file),
                        "--capture-only",
                    ]
                )

            run = json.loads((root / "alias-semantic" / "run.json").read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertEqual(run["command"], "semantic")
        self.assertEqual([model["name"] for model in run["models"]], ["gpt-5.4-mini"])

    def test_task_alias_style_arguments_work_for_dom_repair(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            cases_file = root / "dom-repair-cases.jsonl"
            cases_file.write_text("", encoding="utf-8")

            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = evaluate_llms.main(
                    [
                        "dom-repair",
                        "--models",
                        "kimi-k2p6-fireworks",
                        "--run-name",
                        "alias-dom",
                        "--output-root",
                        str(root),
                        "--cases-file",
                        str(cases_file),
                        "--capture-only",
                    ]
                )

            run = json.loads((root / "alias-dom" / "run.json").read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertEqual(run["command"], "dom-repair")
        self.assertEqual([model["name"] for model in run["models"]], ["kimi-k2p6-fireworks"])

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

    def test_validate_api_url_rejects_non_http_scheme(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "API URL"):
            evaluate_llms.validate_api_url("file:///tmp/model-response.json", "test profile")

    def test_anthropic_request_options_are_merged_into_payload(self) -> None:
        captured: dict[str, object] = {}

        def fake_read_json_response(request: object, *, timeout_seconds: float) -> dict[str, object]:
            captured["payload"] = json.loads(request.data.decode("utf-8"))  # type: ignore[attr-defined]
            captured["timeout_seconds"] = timeout_seconds
            return {"content": [{"type": "text", "text": "{}"}], "usage": {}}

        profile = evaluate_llms.ModelProfile(
            name="claude-test",
            provider="anthropic",
            model="claude-test",
            api_key_env="ANTHROPIC_API_KEY",
            request_options={"max_tokens": "321", "temperature": 0.2, "top_p": 0.9},
        )

        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            with patch.object(evaluate_llms, "read_json_response", side_effect=fake_read_json_response):
                evaluate_llms.call_anthropic_json_model(
                    profile,
                    system="Return JSON.",
                    user_payload={"case": "example"},
                    timeout_seconds=12.5,
                )

        payload = captured["payload"]
        self.assertIsInstance(payload, dict)
        self.assertEqual(payload["max_tokens"], 321)
        self.assertEqual(payload["temperature"], 0.2)
        self.assertEqual(payload["top_p"], 0.9)
        self.assertEqual(captured["timeout_seconds"], 12.5)

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

    def test_semantic_place_type_prefers_cafe_before_generic_food(self) -> None:
        enrichment = EnrichmentPlace(
            primary_type_display_name="Coffee shop",
            types=["food", "point_of_interest"],
        )

        label = evaluate_llms.semantic_place_type_label(enrichment)

        self.assertEqual(label, "cafe")

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

    def test_select_stratified_semantic_cases_tolerates_missing_fixture_labels(self) -> None:
        cases = [
            {"case_id": "a", "fixture_labels": None},
            {"case_id": "b"},
            {"case_id": "c", "fixture_labels": ["evidence:raw-note"]},
        ]

        selected = evaluate_llms.select_stratified_semantic_cases(cases, limit=2)

        self.assertEqual(len(selected), 2)
        self.assertIn("c", {case["case_id"] for case in selected})


if __name__ == "__main__":
    unittest.main()
