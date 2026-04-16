from __future__ import annotations

import json
import unittest

from gmaps_scraper.place_scraper import (
    _build_place_details,
    _extract_address_from_lines,
    _extract_preview_coordinates,
    _extract_preview_place_enrichment,
    _merge_place_sources,
    _parse_review_count,
    _seed_google_consent_cookies,
)


class PlaceScraperTests(unittest.TestCase):
    def test_parse_review_count_handles_suffixes(self) -> None:
        self.assertEqual(_parse_review_count("324"), 324)
        self.assertEqual(_parse_review_count("1,296"), 1296)
        self.assertEqual(_parse_review_count("1.296"), 1296)
        self.assertEqual(_parse_review_count("3.6K"), 3600)
        self.assertEqual(_parse_review_count("9.4万"), 94000)

    def test_build_place_details_uses_dom_fields_and_body_fallbacks(self) -> None:
        details = _build_place_details(
            "https://www.google.com/maps/place/Den",
            resolved_url="https://www.google.com/maps/place/Den/@35.6731762,139.7127216,17z",
            snapshot={
                "name": "Den",
                "secondary_name": "傳",
                "rating": "4.4",
                "review_count": "324",
                "category": "Japanese restaurant",
                "address": (
                    "Japan, 〒150-0001 Tokyo, Shibuya, Jingumae, 2 Chome−3−18 "
                    "建築家会館ＪＩＡ館"
                ),
                "located_in": "Floor 1 · 日本建築家協会",
                "status": "Closed · Opens 6 PM",
                "website": "http://www.jimbochoden.com/",
                "phone": "+81 3-6455-5433",
                "plus_code": "MPF7+73 Shibuya, Tokyo, Japan",
                "limited_view": True,
                "body_text": "\n".join(
                    [
                        "Den",
                        "傳",
                        "4.4",
                        "Japanese restaurant·",
                        (
                            "Seasonal menus of strikingly presented contemporary dishes, "
                            "with wine pairings, in a stylish space."
                        ),
                    ]
                ),
            },
        )

        self.assertEqual(details.name, "Den")
        self.assertEqual(details.secondary_name, "傳")
        self.assertEqual(details.category, "Japanese restaurant")
        self.assertEqual(details.rating, 4.4)
        self.assertEqual(details.review_count, 324)
        self.assertEqual(
            details.description,
            (
                "Seasonal menus of strikingly presented contemporary dishes, with wine "
                "pairings, in a stylish space."
            ),
        )
        self.assertEqual(details.lat, 35.6731762)
        self.assertEqual(details.lng, 139.7127216)
        self.assertTrue(details.limited_view)

    def test_build_place_details_preserves_zero_coordinates(self) -> None:
        details = _build_place_details(
            "https://www.google.com/maps/place/Null+Island",
            resolved_url="https://www.google.com/maps/place/Null+Island",
            snapshot={
                "name": "Null Island",
                "category": "Tourist attraction",
                "lat": 0.0,
                "lng": 0.0,
                "body_text": "Null Island\nTourist attraction",
            },
        )

        self.assertEqual(details.lat, 0.0)
        self.assertEqual(details.lng, 0.0)

    def test_extract_address_from_lines_supports_non_japanese_addresses(self) -> None:
        self.assertEqual(
            _extract_address_from_lines(
                [
                    "Coffee shop",
                    "Open ⋅ Closes 8 PM",
                    "1600 Amphitheatre Parkway, Mountain View, CA 94043",
                ]
            ),
            "1600 Amphitheatre Parkway, Mountain View, CA 94043",
        )

    def test_extract_preview_place_enrichment_backfills_core_fields(self) -> None:
        payload_data = [
            None,
            None,
            None,
            None,
            None,
            None,
            [
                "token",
                "meta",
                [
                    "Japan",
                    "〒150-0001 Tokyo, Shibuya, Jingumae, 2 Chome−3−18",
                    "建築家会館ＪＩＡ館",
                ],
                None,
                [None, None, None, None, None, None, None, 4.4],
                None,
                None,
                ["http://www.jimbochoden.com/", "jimbochoden.com"],
                None,
                [None, None, 35.6731762, 139.7127216],
                "0x60188c981788132b:0x6ef132909b155a88",
                "Den",
                None,
                ["Japanese restaurant", "Kaiseki restaurant", "Restaurant"],
                "2 Chome Jingumae",
                None,
                None,
                None,
                "Japan, 〒150-0001 Tokyo, Shibuya, Jingumae, 2 Chome−3−18 Den, 建築家会館ＪＩＡ館",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                ["Modern setting for fine dining menus", "SearchResult.TYPE_JAPANESE_RESTAURANT"],
                "/g/11c5s9cpnk",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                [["+81 3-6455-5433", [["03-6455-5433", 1], ["+81 3-6455-5433", 2]]]],
                None,
                None,
                None,
                None,
                [
                    [
                        [
                            "2 Chome Jingumae",
                            "Jingumae, 2 Chome−3−18 建築家会館ＪＩＡ館",
                            "Jingumae, 2 Chome−3−18 建築家会館ＪＩＡ館",
                            "Shibuya",
                            "150-0001",
                            "Tokyo",
                            "JP",
                            ["Floor 1"],
                        ],
                        ["0ahUKE", "8Q7XMPF7+73", ["MPF7+73 Shibuya, Tokyo, Japan"], 3],
                    ]
                ],
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                [[None, None, 35.6731762, 139.7127216]],
            ],
        ]
        payload = ")]}'\n" + json.dumps(payload_data, ensure_ascii=False)
        enrichment = _extract_preview_place_enrichment(payload)

        self.assertEqual(enrichment["website"], "http://www.jimbochoden.com/")
        self.assertEqual(enrichment["phone"], "+81 3-6455-5433")
        self.assertEqual(enrichment["plus_code"], "MPF7+73 Shibuya, Tokyo, Japan")
        self.assertEqual(
            enrichment["address"],
            "Japan, 〒150-0001 Tokyo, Shibuya, Jingumae, 2 Chome−3−18 Den, 建築家会館ＪＩＡ館",
        )
        self.assertEqual(enrichment["category"], "Japanese restaurant")
        self.assertEqual(enrichment["description"], "Modern setting for fine dining menus")
        self.assertEqual(enrichment["lat"], 35.6731762)
        self.assertEqual(enrichment["lng"], 139.7127216)

    def test_extract_preview_coordinates_ignores_short_integer_pairs(self) -> None:
        root = [
            [1, 2],
            ["noise", [None, None, 35.6731762, 139.7127216]],
        ]

        self.assertEqual(
            _extract_preview_coordinates(root),
            (35.6731762, 139.7127216),
        )

    def test_merge_place_sources_only_backfills_missing_fields(self) -> None:
        merged = _merge_place_sources(
            {
                "name": "Den",
                "category": "",
                "website": None,
                "phone": "+81 3-6455-5433",
                "limited_view": False,
            },
            {
                "category": "Japanese restaurant",
                "website": "http://www.jimbochoden.com/",
                "phone": "03-6455-5433",
                "limited_view": True,
            },
        )

        self.assertEqual(merged["name"], "Den")
        self.assertEqual(merged["category"], "Japanese restaurant")
        self.assertEqual(merged["website"], "http://www.jimbochoden.com/")
        self.assertEqual(merged["phone"], "+81 3-6455-5433")
        self.assertTrue(merged["limited_view"])

    def test_seed_google_consent_cookies_uses_page_context(self) -> None:
        class _FakeContext:
            def __init__(self) -> None:
                self.cookies: list[object] = []

            def add_cookies(self, cookies: list[object]) -> None:
                self.cookies.extend(cookies)

        class _FakePage:
            def __init__(self) -> None:
                self.context = _FakeContext()

        page = _FakePage()
        _seed_google_consent_cookies(
            page,
            source_url="https://www.google.com/maps/place/Den/@35.6731762,139.7127216,17z",
        )

        self.assertGreaterEqual(len(page.context.cookies), 1)
        self.assertEqual(page.context.cookies[0]["name"], "CONSENT")


if __name__ == "__main__":
    unittest.main()
