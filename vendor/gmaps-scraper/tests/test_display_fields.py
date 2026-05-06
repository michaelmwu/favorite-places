from __future__ import annotations

import unittest

from gmaps_scraper.display_fields import (
    repair_place_display_fields,
    reusable_place_display_fields,
    reuse_place_display_fields,
)
from gmaps_scraper.models import PlaceDetails, PlaceLLMRepairRequest


class DisplayFieldReuseTests(unittest.TestCase):
    def test_reuses_prior_display_fields_when_raw_fields_are_unchanged(self) -> None:
        reusable = reusable_place_display_fields(
            {
                "category": "イタリア料理店",
                "address": "1 The Knolls, シンガポール 098297",
            },
            {
                "category": "イタリア料理店",
                "category_display_en": "Italian restaurant",
                "category_display_en_source": "translation_memory",
                "category_display_en_confidence": "high",
                "address": "1 The Knolls, シンガポール 098297",
                "address_display_en": "1 The Knolls, Singapore 098297",
                "address_display_en_source": "llm",
                "address_display_en_confidence": "high",
            },
        )

        self.assertEqual(
            reusable,
            {
                "category_display_en": "Italian restaurant",
                "category_display_en_source": "translation_memory",
                "category_display_en_confidence": "high",
                "address_display_en": "1 The Knolls, Singapore 098297",
                "address_display_en_source": "llm",
                "address_display_en_confidence": "high",
            },
        )

    def test_rejects_stale_or_non_english_prior_display_fields(self) -> None:
        reusable = reusable_place_display_fields(
            {
                "category": "イタリア料理店",
                "address": "2 The Knolls, シンガポール 098297",
            },
            {
                "category": "イタリア料理店",
                "category_display_en": "イタリア料理店",
                "address": "1 The Knolls, シンガポール 098297",
                "address_display_en": "1 The Knolls, Singapore 098297",
            },
        )

        self.assertEqual(reusable, {})

    def test_preserves_current_good_display_field(self) -> None:
        reusable = reusable_place_display_fields(
            {
                "address": "1 The Knolls, シンガポール 098297",
                "address_display_en": "1 The Knolls, Singapore 098297",
            },
            {
                "address": "1 The Knolls, シンガポール 098297",
                "address_display_en": "1 The Knolls, SG 098297",
            },
        )

        self.assertEqual(reusable, {})

    def test_can_filter_prior_source_and_confidence(self) -> None:
        reusable = reusable_place_display_fields(
            {"address": "1 The Knolls, シンガポール 098297"},
            {
                "address": "1 The Knolls, シンガポール 098297",
                "address_display_en": "1 The Knolls, Singapore 098297",
                "address_display_en_source": "manual",
                "address_display_en_confidence": "high",
            },
            accepted_sources={"llm", "translation_memory"},
            accepted_confidences={"high"},
        )

        self.assertEqual(reusable, {})

    def test_reuse_place_display_fields_returns_copy_with_reusable_fields(self) -> None:
        current = PlaceDetails(
            source_url="https://www.google.com/maps/place/Fiamma",
            resolved_url="https://www.google.com/maps/place/Fiamma",
            name="Fiamma",
            category="イタリア料理店",
            rating=None,
            review_count=None,
            address="1 The Knolls, シンガポール 098297",
        )
        previous = PlaceDetails(
            source_url="https://www.google.com/maps/place/Fiamma",
            resolved_url="https://www.google.com/maps/place/Fiamma",
            name="Fiamma",
            category="イタリア料理店",
            rating=None,
            review_count=None,
            category_display_en="Italian restaurant",
            category_display_en_source="translation_memory",
            category_display_en_confidence="high",
            address="1 The Knolls, シンガポール 098297",
            address_display_en="1 The Knolls, Singapore 098297",
            address_display_en_source="llm",
            address_display_en_confidence="high",
        )

        merged = reuse_place_display_fields(current, previous)

        self.assertIsNot(merged, current)
        self.assertIsNone(current.address_display_en)
        self.assertEqual(merged.category_display_en, "Italian restaurant")
        self.assertEqual(merged.address_display_en, "1 The Knolls, Singapore 098297")

    def test_repair_place_display_fields_uses_llm_without_scraping(self) -> None:
        calls: list[PlaceLLMRepairRequest] = []
        place = PlaceDetails(
            source_url="https://www.google.com/maps/place/Fiamma",
            resolved_url="https://www.google.com/maps/place/Fiamma",
            google_place_id="ChIJfixture",
            name="Fiamma",
            category="イタリア料理店",
            rating=None,
            review_count=None,
            address="1 The Knolls, シンガポール 098297",
            located_in="Capella Singapore",
            address_parts=["1 The Knolls", "シンガポール 098297"],
        )

        def repairer(request: PlaceLLMRepairRequest) -> dict[str, object]:
            calls.append(request)
            return {
                "fields": {
                    "category_display_en": "Italian restaurant",
                    "category_display_en_source": "llm",
                    "category_display_en_confidence": "high",
                    "address_display_en": "1 The Knolls, Singapore 098297",
                    "address_display_en_source": "llm",
                    "address_display_en_confidence": "high",
                    "name": "Ignored",
                }
            }

        repaired = repair_place_display_fields(
            place,
            repairer=repairer,
            evidence={"city": "Singapore", "country": "Singapore"},
        )

        self.assertIsNot(repaired, place)
        self.assertEqual(repaired.category_display_en, "Italian restaurant")
        self.assertEqual(repaired.address_display_en, "1 The Knolls, Singapore 098297")
        self.assertEqual(len(calls), 1)
        request = calls[0]
        self.assertEqual(request.tasks, ["display_translation"])
        self.assertEqual(
            request.diagnostics.quality_flags,
            ["needs_category_display_en", "needs_address_display_en"],
        )
        self.assertEqual(request.current_fields["name"], "Fiamma")
        self.assertEqual(request.current_fields["located_in"], "Capella Singapore")
        self.assertEqual(request.current_fields["google_place_id"], "ChIJfixture")
        self.assertEqual(
            request.evidence["caller_evidence"],
            {"city": "Singapore", "country": "Singapore"},
        )

    def test_repair_place_display_fields_skips_when_display_fields_are_good(self) -> None:
        calls: list[PlaceLLMRepairRequest] = []
        place = PlaceDetails(
            source_url="https://www.google.com/maps/place/Fiamma",
            resolved_url="https://www.google.com/maps/place/Fiamma",
            name="Fiamma",
            category="イタリア料理店",
            category_display_en="Italian restaurant",
            rating=None,
            review_count=None,
            address="1 The Knolls, シンガポール 098297",
            address_display_en="1 The Knolls, Singapore 098297",
        )

        repaired = repair_place_display_fields(
            place,
            repairer=lambda request: calls.append(request) or {},
        )

        self.assertIs(repaired, place)
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
