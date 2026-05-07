from __future__ import annotations

import json
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import gmaps_scraper.translation_memory as translation_memory
from gmaps_scraper.translation_memory import (
    TranslationMemory,
    write_learned_translation_memory,
)


class TranslationMemoryTests(unittest.TestCase):
    def test_default_memory_normalizes_only_known_non_latin_address_components(self) -> None:
        memory = TranslationMemory.default()

        result = memory.normalize_address(
            "245 2층 Itaewon-ro, 한남동 Yongsan District, Seoul, South Korea"
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(
            result.text,
            "245 2F Itaewon-ro, Hannam-dong Yongsan District, Seoul, South Korea",
        )

        ground_floor = memory.normalize_address("1층 Bar Tea Scent")
        self.assertIsNotNone(ground_floor)
        assert ground_floor is not None
        self.assertEqual(ground_floor.text, "1F Bar Tea Scent")

    def test_default_memory_normalizes_basement_address_marker_in_place(self) -> None:
        memory = TranslationMemory.default()

        result = memory.normalize_address("Hong Kong, Central, Lyndhurst Terrace, 48地下")

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(
            result.text,
            "Hong Kong, Central, Lyndhurst Terrace, Basement #48",
        )

    def test_default_memory_applies_approved_address_pattern_templates(self) -> None:
        memory = TranslationMemory.default()

        floor = memory.normalize_address(
            "No. 5之20號, Zhongshan Rd, Liuqiu Township, Pingtung County, Taiwan 929"
        )
        unit = memory.normalize_address("Taiwan, Taipei City, Zhongshan District, 8樓之3")

        self.assertIsNotNone(floor)
        self.assertIsNotNone(unit)
        assert floor is not None
        assert unit is not None
        self.assertEqual(
            floor.text,
            "No. 5-20, Zhongshan Rd, Liuqiu Township, Pingtung County, Taiwan 929",
        )
        self.assertEqual(unit.text, "Taiwan, Taipei City, Zhongshan District, 8F-3")

    def test_file_memory_can_define_approved_address_pattern_templates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "translation-memory.learned.json"
            path.write_text(
                json.dumps(
                    {
                        "entries": [
                            {
                                "kind": "pattern",
                                "source_pattern": r"室(\d+)",
                                "target_template": "Suite {1}",
                                "field_kinds": ["address_component"],
                                "source_method": "approved",
                                "confidence": "high",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            memory = TranslationMemory.from_file(path)
            result = memory.normalize_address("室8 Tung Choi St")

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.text, "Suite 8 Tung Choi St")

    def test_default_memory_leaves_latin_addresses_alone(self) -> None:
        memory = TranslationMemory.default()

        self.assertIsNone(
            memory.normalize_address(
                "73-75 Hàng Điếu, Phố cổ Hà Nội, Hoàn Kiếm, Hà Nội, Vietnam"
            )
        )

    def test_default_memory_normalizes_observed_non_latin_categories(self) -> None:
        memory = TranslationMemory.default()

        cocktail_bar = memory.normalize_category("칵테일바")
        thai_restaurant = memory.normalize_category("タイ料理店")
        bar = memory.normalize_category("酒吧")

        self.assertIsNotNone(cocktail_bar)
        self.assertIsNotNone(thai_restaurant)
        self.assertIsNotNone(bar)
        assert cocktail_bar is not None
        assert thai_restaurant is not None
        assert bar is not None
        self.assertEqual(cocktail_bar.text, "Cocktail bar")
        self.assertEqual(thai_restaurant.text, "Thai restaurant")
        self.assertEqual(bar.text, "Bar")

    def test_learned_memory_reuses_llm_component_translation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "translation-memory.learned.json"
            write_learned_translation_memory(
                path,
                current_fields={"address": "서울시, 강남구"},
                repair={"address_display_en": "Seoul, Gangnam-gu"},
            )

            memory = TranslationMemory.from_file(path)
            result = memory.normalize_address("서울시, 강남구")

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.text, "Seoul, Gangnam-gu")

    def test_learned_memory_writes_are_serialized_for_parallel_calls(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "translation-memory.learned.json"
            original_read = translation_memory._read_pending_payload

            def slow_read(read_path: Path) -> dict[str, object]:
                payload = original_read(read_path)
                time.sleep(0.01)
                return payload

            def write_entry(index: int) -> None:
                write_learned_translation_memory(
                    path,
                    current_fields={"address": f"서울시, 구{index}"},
                    repair={"address_display_en": f"Seoul, District {index}"},
                )

            with patch(
                "gmaps_scraper.translation_memory._read_pending_payload",
                side_effect=slow_read,
            ):
                with ThreadPoolExecutor(max_workers=6) as executor:
                    list(executor.map(write_entry, range(12)))

            payload = json.loads(path.read_text(encoding="utf-8"))

        entries = payload["entries"]
        district_targets = {
            entry["target"]
            for entry in entries
            if isinstance(entry, dict)
            and isinstance(entry.get("source"), str)
            and entry["source"].startswith("구")
        }
        self.assertEqual(
            district_targets,
            {f"District {index}" for index in range(12)},
        )

    def test_learned_memory_does_not_learn_review_text(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "translation-memory.learned.json"
            write_learned_translation_memory(
                path,
                current_fields={"reviews": [{"text": "맛있어요"}]},
                repair={"reviews": [{"text": "Delicious"}]},
            )

            payload = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}

        self.assertEqual(payload, {})


if __name__ == "__main__":
    unittest.main()
