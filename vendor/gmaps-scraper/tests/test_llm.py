from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from gmaps_scraper.llm import (
    cached_place_repairer,
    openai_compatible_place_repairer_from_env,
)
from gmaps_scraper.models import (
    PLACE_LLM_REPAIR_FIELDS,
    PlaceExtractionDiagnostics,
    PlaceLLMRepairRequest,
)


class _FakeHTTPResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload
        self.status = 200

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self) -> _FakeHTTPResponse:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False


def _build_request() -> PlaceLLMRepairRequest:
    return PlaceLLMRepairRequest(
        source_url="https://www.google.com/maps/place/Den",
        resolved_url="https://www.google.com/maps/place/Den",
        current_fields={"name": "Den"},
        diagnostics=PlaceExtractionDiagnostics(quality_flags=["missing_address"]),
        evidence={"dom_excerpt": "Den"},
    )


def _build_address_request(address: str, evidence_hash: str) -> PlaceLLMRepairRequest:
    return PlaceLLMRepairRequest(
        source_url=f"https://www.google.com/maps/place/{evidence_hash}",
        resolved_url=f"https://www.google.com/maps/place/{evidence_hash}",
        current_fields={"name": "Fixture", "address": address},
        diagnostics=PlaceExtractionDiagnostics(
            quality_flags=["needs_address_display_en"],
            evidence_hash=evidence_hash,
            prompt_version="prompt-fixture",
        ),
        evidence={"dom_excerpt": address},
    )


class LLMConfigTests(unittest.TestCase):
    def test_cached_place_repairer_reuses_evidence_hash_results(self) -> None:
        calls: list[PlaceLLMRepairRequest] = []

        def repairer(request: PlaceLLMRepairRequest) -> dict[str, object]:
            calls.append(request)
            return {"address_display_en": "Tokyo, Japan"}

        request = _build_request()
        request.diagnostics.evidence_hash = "evidence-fixture"
        request.diagnostics.prompt_version = "prompt-fixture"

        with tempfile.TemporaryDirectory() as tmp_dir:
            cached = cached_place_repairer(
                repairer,
                cache_dir=Path(tmp_dir),
                cache_namespace="gpt-test",
            )

            first = cached(request)
            second = cached(request)

        self.assertEqual(first, {"address_display_en": "Tokyo, Japan", "_repair_source": "llm"})
        self.assertEqual(
            second,
            {"address_display_en": "Tokyo, Japan", "_repair_source": "cache"},
        )
        self.assertEqual(len(calls), 1)

    def test_cached_place_repairer_reuses_learned_translation_memory(self) -> None:
        calls: list[PlaceLLMRepairRequest] = []

        def repairer(request: PlaceLLMRepairRequest) -> dict[str, object]:
            calls.append(request)
            return {"address_display_en": "Seoul, Gangnam-gu"}

        with tempfile.TemporaryDirectory() as tmp_dir:
            cached = cached_place_repairer(
                repairer,
                cache_dir=Path(tmp_dir),
                cache_namespace="gpt-test",
            )

            first = cached(_build_address_request("서울시, 강남구", "evidence-one"))
            second = cached(_build_address_request("서울시, 강남구", "evidence-two"))

        self.assertEqual(
            first,
            {"address_display_en": "Seoul, Gangnam-gu", "_repair_source": "llm"},
        )
        self.assertEqual(
            second,
            {
                "fields": {
                    "address_display_en": "Seoul, Gangnam-gu",
                    "address_display_en_source": "translation_memory",
                    "address_display_en_confidence": "medium",
                },
                "_repair_source": "translation_memory",
            },
        )
        self.assertEqual(len(calls), 1)

    def test_cached_translation_memory_hit_does_not_require_model_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache_dir = Path(tmp_dir)
            (cache_dir / "translation-memory.learned.json").write_text(
                json.dumps(
                    {
                        "entries": [
                            {
                                "source": "서울시, 강남구",
                                "target": "Seoul, Gangnam-gu",
                                "field_kinds": ["address"],
                                "source_method": "learned",
                                "confidence": "medium",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            with patch.dict("os.environ", {}, clear=True):
                repairer = openai_compatible_place_repairer_from_env(
                    env_file=cache_dir / ".env",
                    default_config_file=cache_dir / "llm.json",
                    local_config_file=cache_dir / "llm.local.json",
                )
                cached = cached_place_repairer(
                    repairer,
                    cache_dir=cache_dir,
                    cache_namespace="gpt-test",
                )
                result = cached(_build_address_request("서울시, 강남구", "evidence-one"))

        self.assertEqual(
            result,
            {
                "fields": {
                    "address_display_en": "Seoul, Gangnam-gu",
                    "address_display_en_source": "translation_memory",
                    "address_display_en_confidence": "medium",
                },
                "_repair_source": "translation_memory",
            },
        )

    def test_gpt_5_mini_uses_checked_in_defaults_and_omits_temperature(self) -> None:
        captured: dict[str, object] = {}

        def fake_urlopen(http_request: object, timeout: float) -> _FakeHTTPResponse:
            del timeout
            request_data = json.loads(http_request.data.decode("utf-8"))
            captured["url"] = http_request.full_url
            captured["payload"] = request_data
            return _FakeHTTPResponse(
                {"choices": [{"message": {"content": "{}"}}]}
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            env_path = Path(tmp_dir) / ".env"
            env_path.write_text(
                "OPENAI_API_KEY=test-openai-key\nLLM_MODEL=gpt-5-mini\n",
                encoding="utf-8",
            )
            local_config_path = Path(tmp_dir) / ".llm.local.json"
            with (
                patch.dict("os.environ", {}, clear=True),
                patch("urllib.request.urlopen", side_effect=fake_urlopen),
            ):
                repair = openai_compatible_place_repairer_from_env(
                    env_file=env_path,
                    local_config_file=local_config_path,
                )
                response = repair(_build_request())

        self.assertEqual(response, {})
        self.assertEqual(captured["url"], "https://api.openai.com/v1/chat/completions")
        payload = captured["payload"]
        self.assertEqual(payload["model"], "gpt-5-mini")
        self.assertEqual(payload["response_format"], {"type": "json_object"})
        self.assertNotIn("temperature", payload)
        messages = payload["messages"]
        self.assertIsInstance(messages, list)
        user_message = messages[1]
        self.assertIsInstance(user_message, dict)
        content = user_message["content"]
        self.assertIsInstance(content, str)
        self.assertEqual(
            json.loads(content)["allowed_fields"],
            list(PLACE_LLM_REPAIR_FIELDS),
        )

    def test_local_config_can_define_fireworks_alias(self) -> None:
        captured: dict[str, object] = {}

        def fake_urlopen(http_request: object, timeout: float) -> _FakeHTTPResponse:
            del timeout
            captured["url"] = http_request.full_url
            captured["payload"] = json.loads(http_request.data.decode("utf-8"))
            return _FakeHTTPResponse(
                {"choices": [{"message": {"content": "{}"}}]}
            )

        local_config = {
            "providers": {
                "fireworks": {
                    "api_key_env": "FIREWORKS_API_KEY",
                    "base_url": "https://api.fireworks.ai/inference/v1",
                }
            },
            "models": {
                "kimi-k2p6": {
                    "provider": "fireworks",
                    "model": "accounts/fireworks/models/kimi-k2p6",
                    "omit_temperature": True,
                    "request_options": {
                        "reasoning_effort": "low",
                        "temperature": 0,
                    },
                }
            },
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            env_path = Path(tmp_dir) / ".env"
            env_path.write_text(
                "FIREWORKS_API_KEY=test-fireworks-key\nLLM_MODEL=kimi-k2p6\n",
                encoding="utf-8",
            )
            local_config_path = Path(tmp_dir) / ".llm.local.json"
            local_config_path.write_text(json.dumps(local_config), encoding="utf-8")
            with (
                patch.dict("os.environ", {}, clear=True),
                patch("urllib.request.urlopen", side_effect=fake_urlopen),
            ):
                repair = openai_compatible_place_repairer_from_env(
                    env_file=env_path,
                    local_config_file=local_config_path,
                )
                response = repair(_build_request())

        self.assertEqual(response, {})
        self.assertEqual(
            captured["url"],
            "https://api.fireworks.ai/inference/v1/chat/completions",
        )
        payload = captured["payload"]
        self.assertEqual(payload["model"], "accounts/fireworks/models/kimi-k2p6")
        self.assertEqual(payload["reasoning_effort"], "low")
        self.assertNotIn("temperature", payload)

    def test_env_vars_override_local_request_options(self) -> None:
        captured: dict[str, object] = {}

        def fake_urlopen(http_request: object, timeout: float) -> _FakeHTTPResponse:
            del timeout
            captured["payload"] = json.loads(http_request.data.decode("utf-8"))
            return _FakeHTTPResponse(
                {"choices": [{"message": {"content": "{}"}}]}
            )

        local_config = {
            "models": {
                "gpt-4o-mini": {
                    "provider": "openai",
                    "model": "gpt-4o-mini",
                    "request_options": {
                        "temperature": 0.2,
                        "max_tokens": 256,
                    },
                }
            }
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            env_path = Path(tmp_dir) / ".env"
            env_path.write_text(
                "OPENAI_API_KEY=test-openai-key\nLLM_MODEL=gpt-4o-mini\n",
                encoding="utf-8",
            )
            local_config_path = Path(tmp_dir) / ".llm.local.json"
            local_config_path.write_text(json.dumps(local_config), encoding="utf-8")
            with (
                patch.dict(
                    "os.environ",
                    {
                        "LLM_TEMPERATURE": "0.7",
                        "LLM_MAX_TOKENS": "128",
                    },
                    clear=True,
                ),
                patch("urllib.request.urlopen", side_effect=fake_urlopen),
            ):
                repair = openai_compatible_place_repairer_from_env(
                    env_file=env_path,
                    local_config_file=local_config_path,
                )
                response = repair(_build_request())

        self.assertEqual(response, {})
        payload = captured["payload"]
        self.assertEqual(payload["temperature"], 0.7)
        self.assertEqual(payload["max_tokens"], 128)


if __name__ == "__main__":
    unittest.main()
