from __future__ import annotations

import json
import unittest

from gmaps_scraper.place_scraper import (
    _build_place_details,
    _clean_category_text,
    _clean_name_text,
    _extract_address_from_lines,
    _extract_preview_coordinates,
    _extract_preview_description,
    _extract_preview_phone,
    _extract_preview_place_enrichment,
    _extract_secondary_name,
    _merge_place_sources,
    _normalize_phone_candidate,
    _normalize_preview_website,
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

    def test_clean_name_text_preserves_names_that_start_with_open_or_closed(self) -> None:
        self.assertEqual(_clean_name_text("Open Kitchen"), "Open Kitchen")
        self.assertEqual(_clean_name_text("Closed Loop Coffee"), "Closed Loop Coffee")
        self.assertEqual(_clean_name_text("Open Now Cafe"), "Open Now Cafe")
        self.assertIsNone(_clean_name_text("Open ⋅ Closes 8 PM"))
        self.assertIsNone(_clean_name_text("Open now"))

    def test_clean_category_text_rejects_search_result_labels(self) -> None:
        self.assertIsNone(_clean_category_text("share"))
        self.assertIsNone(_clean_category_text("結果"))
        self.assertEqual(_clean_category_text("Japanese restaurant"), "Japanese restaurant")

    def test_clean_name_text_preserves_exact_share_name(self) -> None:
        self.assertEqual(_clean_name_text("Share"), "Share")

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

    def test_extract_preview_description_preserves_text_starting_with_open(self) -> None:
        description = _extract_preview_description(
            [
                "Open fire cooking over binchotan.",
                "Open ⋅ Closes 10 PM",
                "SearchResult.TYPE_RESTAURANT",
            ]
        )

        self.assertEqual(
            description,
            "Open fire cooking over binchotan.",
        )

    def test_extract_preview_description_preserves_open_now_prose(self) -> None:
        description = _extract_preview_description(
            [
                "Open now for lunch and dinner service.",
                "Open now ⋅ Closes 10 PM",
            ]
        )

        self.assertEqual(description, "Open now for lunch and dinner service.")

    def test_extract_preview_coordinates_ignores_short_integer_pairs(self) -> None:
        root = [
            [1, 2],
            ["noise", [None, None, 35.6731762, 139.7127216]],
        ]

        self.assertEqual(
            _extract_preview_coordinates(root),
            (35.6731762, 139.7127216),
        )

    def test_extract_preview_phone_rejects_cid_like_values(self) -> None:
        self.assertEqual(
            _extract_preview_phone(["5180951040094558101", "1776609428996", "+33 1 42 00 00 00"]),
            "+33 1 42 00 00 00",
        )

    def test_normalize_phone_candidate_accepts_long_unformatted_international_numbers(self) -> None:
        self.assertEqual(_normalize_phone_candidate("442071838750"), "442071838750")

    def test_build_place_details_ignores_placeholder_name_invalid_phone_and_status_description(
        self,
    ) -> None:
        details = _build_place_details(
            "https://maps.google.com/?cid=5180951040094558101",
            resolved_url="https://www.google.com/maps/place//@48.8814703,2.340862,17z/data=!3m1!4b1",
            snapshot={
                "name": "",
                "secondary_name": "",
                "phone": "5180951040094558101",
                "status": "営業時間外 · 営業開始: 18:00\uFF08火\uFF09",
                "description": "営業時間外 · 営業開始: 18:00\uFF08火\uFF09",
                "lat": 48.8814703,
                "lng": 2.340862,
                "body_text": "\n".join(["", "", "営業時間外 · 営業開始: 18:00\uFF08火\uFF09"]),
            },
        )

        self.assertIsNone(details.name)
        self.assertIsNone(details.secondary_name)
        self.assertIsNone(details.phone)
        self.assertIsNone(details.description)

    def test_build_place_details_rejects_placeholder_description_direct_value(self) -> None:
        details = _build_place_details(
            "https://www.google.com/maps/place/Bianchetto",
            resolved_url="https://www.google.com/maps/place/Bianchetto",
            snapshot={
                "name": "Bianchetto",
                "description": "Share",
                "body_text": "Bianchetto\nRestaurant",
            },
        )

        self.assertIsNone(details.description)

    def test_build_place_details_rejects_search_results_labels_and_rating_categories(self) -> None:
        details = _build_place_details(
            "https://www.google.com/maps/search/?api=1&query=Bianchetto",
            resolved_url="https://www.google.com/maps/search/?api=1&query=Bianchetto",
            snapshot={
                "name": "結果",
                "category": "5.0(8)",
                "address": "バー · 26-28 Cotham Rd",
                "body_text": "\n".join(["結果", "5.0(8)", "バー · 26-28 Cotham Rd"]),
            },
        )

        self.assertIsNone(details.name)

    def test_build_place_details_preserves_numeric_only_name(self) -> None:
        details = _build_place_details(
            "https://www.google.com/maps/place/404",
            resolved_url="https://www.google.com/maps/place/404",
            snapshot={
                "name": "404",
                "body_text": "\n".join(["404", "Bar"]),
            },
        )

        self.assertEqual(details.name, "404")

    def test_build_place_details_preserves_slashed_numeric_name(self) -> None:
        details = _build_place_details(
            "https://www.google.com/maps/place/24-7",
            resolved_url="https://www.google.com/maps/place/24-7",
            snapshot={
                "name": "24/7",
                "body_text": "\n".join(["24/7", "Diner"]),
            },
        )

        self.assertEqual(details.name, "24/7")

    def test_build_place_details_preserves_open_prefixed_name_and_description(self) -> None:
        details = _build_place_details(
            "https://www.google.com/maps/place/Open+Kitchen",
            resolved_url="https://www.google.com/maps/place/Open+Kitchen",
            snapshot={
                "name": "Open Kitchen",
                "description": "Open fire cooking in a bright room.",
                "body_text": "\n".join(
                    [
                        "Open Kitchen",
                        "Restaurant",
                        "Open fire cooking in a bright room.",
                    ]
                ),
            },
        )

        self.assertEqual(details.name, "Open Kitchen")
        self.assertEqual(details.description, "Open fire cooking in a bright room.")
        self.assertIsNone(details.status)

    def test_build_place_details_preserves_photo_url(self) -> None:
        details = _build_place_details(
            "https://www.google.com/maps/place/Open+Kitchen",
            resolved_url="https://www.google.com/maps/place/Open+Kitchen",
            snapshot={
                "name": "Open Kitchen",
                "main_photo_url": "https://lh3.googleusercontent.com/p/main-example=s680-w680-h510",
                "photo_url": "https://lh3.googleusercontent.com/p/example=s680-w680-h510",
                "body_text": "Open Kitchen",
            },
        )

        self.assertEqual(
            details.main_photo_url,
            "https://lh3.googleusercontent.com/p/main-example=s680-w680-h510",
        )
        self.assertEqual(
            details.photo_url,
            "https://lh3.googleusercontent.com/p/example=s680-w680-h510",
        )

    def test_build_place_details_rejects_street_view_as_photo(self) -> None:
        details = _build_place_details(
            "https://www.google.com/maps/place/Open+Kitchen",
            resolved_url="https://www.google.com/maps/place/Open+Kitchen",
            snapshot={
                "name": "Open Kitchen",
                "main_photo_url": (
                    "https://streetviewpixels-pa.googleapis.com/v1/thumbnail?panoid=abc"
                ),
                "photo_url": (
                    "https://streetviewpixels-pa.googleapis.com/v1/thumbnail?panoid=abc"
                ),
                "body_text": "Open Kitchen",
            },
        )

        self.assertIsNone(details.main_photo_url)
        self.assertIsNone(details.photo_url)

    def test_extract_secondary_name_aborts_when_rating_line_follows_name(self) -> None:
        self.assertIsNone(
            _extract_secondary_name(
                ["Den", "4.4", "傳"],
                name="Den",
            )
        )

    def test_normalize_preview_website_rejects_streetview_thumbnail_urls(self) -> None:
        self.assertIsNone(
            _normalize_preview_website(
                "https://streetviewpixels-pa.googleapis.com/v1/thumbnail?panoid=abc"
            )
        )
        self.assertIsNone(
            _normalize_preview_website(
                "https://inline.app/booking/foo?utm_source=ig"
            )
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
