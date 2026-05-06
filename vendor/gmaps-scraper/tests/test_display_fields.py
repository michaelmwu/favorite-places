from __future__ import annotations

import unittest

from gmaps_scraper.display_fields import (
    reusable_place_display_fields,
    reuse_place_display_fields,
)
from gmaps_scraper.models import PlaceDetails


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


if __name__ == "__main__":
    unittest.main()
