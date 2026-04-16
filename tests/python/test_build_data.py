from __future__ import annotations

import json
import os
import unittest
from contextlib import redirect_stderr
from datetime import UTC, datetime, timedelta
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from pydantic import ValidationError

from scripts import build_data
from scripts.pipeline_models import (
    EnrichmentCacheEntry,
    EnrichmentPlace,
    NormalizedPlace,
    RawPlace,
    RawSavedList,
    SourceConfig,
)


class BuildDataTests(unittest.TestCase):
    def test_parser_rejects_negative_refresh_retry_values(self) -> None:
        invalid_args = [
            ["--refresh-retries", "-1"],
            ["--refresh-retry-backoff-seconds", "-0.1"],
            ["--refresh-startup-jitter-seconds", "-0.1"],
        ]

        for args in invalid_args:
            with self.subTest(args=args):
                parser = build_data.build_parser()
                stderr = StringIO()
                with redirect_stderr(stderr), self.assertRaises(SystemExit) as raised:
                    parser.parse_args(args)

                self.assertEqual(raised.exception.code, 2)
                self.assertIn("must be >= 0", stderr.getvalue())

    def test_source_config_infers_google_list_url_from_maps_shortlink(self) -> None:
        source = SourceConfig(slug="florence-italy", url="https://maps.app.goo.gl/mXQoUYRRjWuj6HNw8")

        self.assertEqual(source.type, "google_list_url")

    def test_source_config_accepts_matching_explicit_type(self) -> None:
        source = SourceConfig(
            slug="florence-italy",
            type="google_list_url",
            url="https://maps.app.goo.gl/mXQoUYRRjWuj6HNw8",
        )

        self.assertEqual(source.type, "google_list_url")

    def test_source_config_infers_google_list_url_from_google_maps_share_url(self) -> None:
        source = SourceConfig(
            slug="florence-italy",
            url=(
                "https://www.google.com/maps/@43.7704158,11.2457711,15z/"
                "data=!4m6!1m2!10m1!1e1!11m2!2sMc3UGukaEH6YkxkW4hLKEg!3e3"
            ),
        )

        self.assertEqual(source.type, "google_list_url")

    def test_source_config_infers_google_export_csv_from_path(self) -> None:
        source = SourceConfig(
            slug="taipei-taiwan",
            path="data/imports/taipei-taiwan.csv",
            title="Taipei, Taiwan",
        )

        self.assertEqual(source.type, "google_export_csv")

    def test_source_config_rejects_type_that_disagrees_with_maps_url(self) -> None:
        with self.assertRaises(ValidationError) as context:
            SourceConfig(
                slug="florence-italy",
                type="google_export_csv",
                url="https://maps.app.goo.gl/mXQoUYRRjWuj6HNw8",
            )

        self.assertIn("does not match configured source fields", str(context.exception))

    def test_source_config_rejects_type_that_disagrees_with_csv_path(self) -> None:
        with self.assertRaises(ValidationError) as context:
            SourceConfig(
                slug="taipei-taiwan",
                type="google_list_url",
                path="data/imports/taipei-taiwan.csv",
                title="Taipei, Taiwan",
            )

        self.assertIn("does not match configured source fields", str(context.exception))

    def test_source_config_rejects_non_google_url_for_google_list_url(self) -> None:
        with self.assertRaises(ValidationError) as context:
            SourceConfig(
                slug="example",
                type="google_list_url",
                url="https://example.com/list",
            )

        self.assertIn("supported Google Maps URL", str(context.exception))

    def test_source_config_rejects_google_mymaps_url(self) -> None:
        with self.assertRaises(ValidationError) as context:
            SourceConfig(
                slug="valencia-spain",
                url=(
                    "https://www.google.com/maps/d/u/0/viewer?hl=en"
                    "&mid=1OiyJQ0xcbnanQ2QJsTavzetf5b_D_s4"
                ),
            )

        self.assertIn("Google My Maps URLs are not supported", str(context.exception))

    def test_normalize_guide_applies_overrides_and_hides_places_from_counts(self) -> None:
        raw = RawSavedList(
            title="Tokyo, Japan",
            description="Local favorites. #food #coffee",
            source_url="https://maps.app.goo.gl/tokyo",
            places=[
                RawPlace(
                    name="Coffee House",
                    address="1 Shibuya, Tokyo, Japan",
                    note="Original note",
                    is_favorite=False,
                    lat=35.65,
                    lng=139.7,
                    maps_url="https://maps.google.com/?cid=1",
                    cid="111",
                ),
                RawPlace(
                    name="Noodle Shop",
                    address="2 Shinjuku, Tokyo, Japan",
                    note="Best at lunch",
                    is_favorite=True,
                    lat=35.66,
                    lng=139.71,
                    maps_url="https://maps.google.com/?cid=2",
                    cid="222",
                ),
                RawPlace(
                    name="Hidden Bar",
                    address="3 Ebisu, Tokyo, Japan",
                    maps_url="https://maps.google.com/?cid=3",
                    cid="333",
                ),
            ],
        )
        first_place_id = build_data.stable_place_id(raw.places[0])
        second_place_id = build_data.stable_place_id(raw.places[1])
        third_place_id = build_data.stable_place_id(raw.places[2])

        enrichment_cache = {
            first_place_id: EnrichmentCacheEntry(
                fetched_at="2026-04-01T00:00:00+00:00",
                query="Coffee House, Tokyo",
                matched=True,
                score=88,
                place=EnrichmentPlace(
                    google_maps_uri="https://maps.google.com/?cid=override",
                    primary_type="cafe",
                    primary_type_display_name="Coffee shop",
                    formatted_address="1 Shibuya, Tokyo, Japan",
                    types=["cafe", "food"],
                    business_status="OPERATIONAL",
                ),
            )
        }

        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            list_overrides_dir = tmpdir_path / "lists"
            place_overrides_dir = tmpdir_path / "places"
            list_overrides_dir.mkdir()
            place_overrides_dir.mkdir()

            (list_overrides_dir / "tokyo-japan.json").write_text(
                json.dumps(
                    {
                        "title": "Michael's Tokyo",
                        "city_name": "Tokyo",
                        "country_name": "Japan",
                        "country_code": "JP",
                        "list_tags": ["manual-tag"],
                        "featured_place_ids": [second_place_id],
                    }
                ),
                encoding="utf-8",
            )
            (place_overrides_dir / "tokyo-japan.json").write_text(
                json.dumps(
                    {
                        first_place_id: {
                            "top_pick": True,
                            "note": "Manual note",
                            "primary_category": "Bakery",
                            "tags": ["specialty"],
                            "manual_rank": 3,
                        },
                        third_place_id: {"hidden": True},
                    }
                ),
                encoding="utf-8",
            )

            with (
                patch.object(build_data, "LIST_OVERRIDES_DIR", list_overrides_dir),
                patch.object(build_data, "PLACE_OVERRIDES_DIR", place_overrides_dir),
            ):
                guide = build_data.normalize_guide(
                    "tokyo-japan",
                    raw,
                    enrichment_cache=enrichment_cache,
                )

        self.assertEqual(guide.title, "Michael's Tokyo")
        self.assertEqual(guide.country_name, "Japan")
        self.assertEqual(guide.city_name, "Tokyo")
        self.assertEqual(guide.list_tags, ["coffee", "food", "manual-tag"])
        self.assertEqual(guide.place_count, 2)
        self.assertEqual(guide.featured_place_ids, [second_place_id, first_place_id])
        self.assertAlmostEqual(guide.center_lat or 0, 35.655, places=3)
        self.assertAlmostEqual(guide.center_lng or 0, 139.705, places=3)

        first_place = guide.places[0]
        hidden_place = next(place for place in guide.places if place.id == third_place_id)

        self.assertEqual(first_place.id, first_place_id)
        self.assertEqual(first_place.primary_category, "Bakery")
        self.assertEqual(first_place.note, "Manual note")
        self.assertEqual(first_place.maps_url, "https://maps.google.com/?cid=override")
        self.assertEqual(first_place.neighborhood, "Shibuya")
        self.assertTrue(first_place.top_pick)
        self.assertIn("local-favorite", first_place.vibe_tags)
        self.assertIn("quick-stop", first_place.vibe_tags)
        self.assertEqual(first_place.status, "active")
        self.assertIn("bakery", first_place.tags)
        self.assertIn("shibuya", first_place.tags)
        self.assertIn("specialty", first_place.tags)
        self.assertIn("tokyo", first_place.tags)
        self.assertEqual(first_place.provenance.name.source, "google_list")
        self.assertEqual(first_place.provenance.address.source, "google_list")
        self.assertEqual(first_place.provenance.maps_url.source, "google_places")
        self.assertEqual(first_place.provenance.primary_category.source, "manual")
        self.assertEqual(first_place.provenance.note.source, "manual")
        self.assertEqual(first_place.provenance.top_pick.source, "manual")
        self.assertEqual(first_place.provenance.status.source, "google_places")
        self.assertEqual(
            {field.value: field.source for field in first_place.provenance.tags},
            {
                "specialty": "manual",
                "tokyo": "google_list",
                "shibuya": "google_list",
                "bakery": "manual",
                "cafe": "google_places",
                "food": "google_places",
            },
        )
        self.assertTrue(hidden_place.hidden)

    def test_place_vibe_tags_can_be_manually_overridden(self) -> None:
        raw = RawSavedList(
            title="Tokyo, Japan",
            places=[
                RawPlace(
                    name="Quiet Coffee",
                    address="1 Shibuya, Tokyo, Japan",
                    note="Quiet cafe with wifi and outlets.",
                    maps_url="https://maps.google.com/?cid=1",
                    cid="111",
                ),
            ],
        )
        place_id = build_data.stable_place_id(raw.places[0])

        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            list_overrides_dir = tmpdir_path / "lists"
            place_overrides_dir = tmpdir_path / "places"
            list_overrides_dir.mkdir()
            place_overrides_dir.mkdir()
            (place_overrides_dir / "tokyo-japan.json").write_text(
                json.dumps({place_id: {"vibe_tags": ["date-night"]}}),
                encoding="utf-8",
            )

            with (
                patch.object(build_data, "LIST_OVERRIDES_DIR", list_overrides_dir),
                patch.object(build_data, "PLACE_OVERRIDES_DIR", place_overrides_dir),
            ):
                guide = build_data.normalize_guide(
                    "tokyo-japan",
                    raw,
                    enrichment_cache={},
                )

        self.assertEqual(guide.places[0].vibe_tags, ["date-night"])

    def test_place_vibe_tags_can_be_manually_cleared(self) -> None:
        raw = RawSavedList(
            title="Tokyo, Japan",
            places=[
                RawPlace(
                    name="Quiet Coffee",
                    address="1 Shibuya, Tokyo, Japan",
                    note="Quiet cafe with wifi and outlets.",
                    maps_url="https://maps.google.com/?cid=1",
                    cid="111",
                ),
            ],
        )
        place_id = build_data.stable_place_id(raw.places[0])

        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            list_overrides_dir = tmpdir_path / "lists"
            place_overrides_dir = tmpdir_path / "places"
            list_overrides_dir.mkdir()
            place_overrides_dir.mkdir()
            (place_overrides_dir / "tokyo-japan.json").write_text(
                json.dumps({place_id: {"vibe_tags": []}}),
                encoding="utf-8",
            )

            with (
                patch.object(build_data, "LIST_OVERRIDES_DIR", list_overrides_dir),
                patch.object(build_data, "PLACE_OVERRIDES_DIR", place_overrides_dir),
            ):
                guide = build_data.normalize_guide(
                    "tokyo-japan",
                    raw,
                    enrichment_cache={},
                )

        self.assertEqual(guide.places[0].vibe_tags, [])

    def test_page_backed_enrichment_uses_google_maps_page_provenance(self) -> None:
        raw = RawSavedList(
            title="Tokyo, Japan",
            places=[
                RawPlace(
                    name="Coffee House",
                    address="1 Shibuya, Tokyo, Japan",
                    maps_url="https://maps.google.com/?cid=1",
                    cid="111",
                ),
            ],
        )
        place_id = build_data.stable_place_id(raw.places[0])
        enrichment_cache = {
            place_id: EnrichmentCacheEntry(
                fetched_at="2026-04-01T00:00:00+00:00",
                refresh_after="2026-04-08T00:00:00+00:00",
                source="google_maps_page",
                query="Coffee House, Tokyo",
                matched=True,
                score=45,
                place=EnrichmentPlace(
                    display_name="Coffee House",
                    formatted_address="1 Shibuya, Tokyo, Japan",
                    google_maps_uri="https://www.google.com/maps/place/Coffee+House",
                    primary_type="coffee_shop",
                    primary_type_display_name="Coffee shop",
                    types=["coffee_shop"],
                ),
            )
        }

        guide = build_data.normalize_guide("tokyo-japan", raw, enrichment_cache=enrichment_cache)
        place = guide.places[0]

        self.assertEqual(place.maps_url, "https://www.google.com/maps/place/Coffee+House")
        self.assertEqual(place.provenance.maps_url.source, "google_maps_page")
        self.assertEqual(place.provenance.primary_category.source, "google_maps_page")
        self.assertIn(
            "google_maps_page",
            {field.source for field in place.provenance.tags},
        )

    def test_vibe_tags_match_snake_case_enrichment_types(self) -> None:
        vibes = build_data.derive_vibe_tags(
            RawPlace(
                name="Plain Place",
                address="1 Shibuya, Tokyo, Japan",
                maps_url="https://maps.google.com/?cid=1",
            ),
            enrichment=EnrichmentPlace(primary_type="coffee_shop", types=["coffee_shop"]),
            category="coffee_shop",
            tags=[],
            note=None,
            why_recommended=None,
            top_pick=False,
        )

        self.assertIn("cozy", vibes)
        self.assertIn("slow-afternoon", vibes)
        self.assertIn("solo-friendly", vibes)

    def test_vibe_keyword_matching_uses_token_boundaries(self) -> None:
        self.assertFalse(build_data.vibe_keyword_matches("barcelona cafe", "bar"))
        self.assertTrue(build_data.vibe_keyword_matches("quiet bar seating", "bar"))

    def test_build_search_index_contains_compact_place_context(self) -> None:
        guide = build_data.normalize_guide(
            "tokyo-japan",
            RawSavedList(
                title="Tokyo, Japan",
                places=[
                    RawPlace(
                        name="Quiet Coffee",
                        address="1 Shibuya, Tokyo, Japan",
                        note="Quiet cafe with wifi.",
                        is_favorite=True,
                        maps_url="https://maps.google.com/?cid=1",
                        cid="111",
                    ),
                ],
            ),
            enrichment_cache={},
        )

        index = build_data.build_search_index([guide])

        self.assertEqual(index["version"], 1)
        self.assertEqual(index["guides"][0]["slug"], "tokyo-japan")
        self.assertEqual(index["entries"][0]["guide_slug"], "tokyo-japan")
        self.assertEqual(index["entries"][0]["name"], "Quiet Coffee")
        self.assertIn("quiet", index["entries"][0]["vibe_tags"])
        self.assertIn("laptop-friendly", index["entries"][0]["vibe_tags"])
        self.assertIn("tokyo", index["entries"][0]["search_text"])

    def test_guide_location_center_excludes_far_outliers(self) -> None:
        places = [
            NormalizedPlace(
                id="tokyo-1",
                name="Tokyo 1",
                lat=35.66,
                lng=139.7,
                maps_url="https://maps.example/tokyo-1",
                status="active",
            ),
            NormalizedPlace(
                id="tokyo-2",
                name="Tokyo 2",
                lat=35.67,
                lng=139.71,
                maps_url="https://maps.example/tokyo-2",
                status="active",
            ),
            NormalizedPlace(
                id="tokyo-3",
                name="Tokyo 3",
                lat=35.65,
                lng=139.69,
                maps_url="https://maps.example/tokyo-3",
                status="active",
            ),
            NormalizedPlace(
                id="bad-import",
                name="Bad import",
                lat=48.86,
                lng=2.35,
                maps_url="https://maps.example/bad-import",
                status="active",
            ),
        ]

        center_lat, center_lng = build_data.guide_location_center(places)

        self.assertAlmostEqual(center_lat or 0, 35.66, places=3)
        self.assertAlmostEqual(center_lng or 0, 139.7, places=3)

    def test_warn_far_map_pins_prints_cli_warning_for_distant_places(self) -> None:
        places = [
            NormalizedPlace(
                id="tokyo-1",
                name="Tokyo 1",
                lat=35.66,
                lng=139.7,
                maps_url="https://maps.example/tokyo-1",
                status="active",
            ),
            NormalizedPlace(
                id="bad-import",
                name="Bad import",
                lat=48.86,
                lng=2.35,
                maps_url="https://maps.example/bad-import",
                status="active",
            ),
        ]

        with patch("builtins.print") as print_mock:
            build_data.warn_far_map_pins("tokyo-japan", places, (35.665, 139.705))

        self.assertEqual(print_mock.call_count, 1)
        warning = print_mock.call_args.args[0]
        self.assertIn("WARNING: tokyo-japan:bad-import", warning)
        self.assertIn("check whether it belongs in this city/country", warning)

    def test_guide_map_pin_warning_distance_uses_inlier_radius_plus_buffer(self) -> None:
        places = [
            NormalizedPlace(
                id="urumqi-1",
                name="Urumqi 1",
                lat=43.811,
                lng=87.606,
                maps_url="https://maps.example/urumqi-1",
                status="active",
            ),
            NormalizedPlace(
                id="urumqi-2",
                name="Urumqi 2",
                lat=43.825,
                lng=87.615,
                maps_url="https://maps.example/urumqi-2",
                status="active",
            ),
            NormalizedPlace(
                id="urumqi-3",
                name="Urumqi 3",
                lat=43.801,
                lng=87.649,
                maps_url="https://maps.example/urumqi-3",
                status="active",
            ),
            NormalizedPlace(
                id="urumqi-4",
                name="Urumqi 4",
                lat=43.789,
                lng=87.632,
                maps_url="https://maps.example/urumqi-4",
                status="active",
            ),
            NormalizedPlace(
                id="nalati",
                name="Nalati Grassland",
                lat=43.292522,
                lng=84.229038,
                maps_url="https://maps.example/nalati",
                status="active",
            ),
        ]

        center = build_data.guide_location_center(places)
        threshold = build_data.guide_map_pin_warning_distance_meters(places, center)

        self.assertIsNotNone(threshold)
        self.assertEqual(threshold, build_data.MAP_PIN_DISTANCE_WARNING_MIN_METERS)

    def test_warn_far_map_pins_warns_for_medium_distance_bad_imports(self) -> None:
        places = [
            NormalizedPlace(
                id="tokyo-1",
                name="Tokyo 1",
                lat=35.6600,
                lng=139.7000,
                maps_url="https://maps.example/tokyo-1",
                status="active",
            ),
            NormalizedPlace(
                id="tokyo-2",
                name="Tokyo 2",
                lat=35.6760,
                lng=139.6990,
                maps_url="https://maps.example/tokyo-2",
                status="active",
            ),
            NormalizedPlace(
                id="tokyo-3",
                name="Tokyo 3",
                lat=35.6700,
                lng=139.7350,
                maps_url="https://maps.example/tokyo-3",
                status="active",
            ),
            NormalizedPlace(
                id="tokyo-4",
                name="Tokyo 4",
                lat=35.6890,
                lng=139.6910,
                maps_url="https://maps.example/tokyo-4",
                status="active",
            ),
            NormalizedPlace(
                id="beaumont-import",
                name="Kasama Restaurant",
                lat=33.9290518,
                lng=-116.9776347,
                maps_url="https://maps.example/beaumont-import",
                status="active",
            ),
        ]

        center = build_data.guide_location_center(places)

        with patch("builtins.print") as print_mock:
            build_data.warn_far_map_pins("tokyo-japan", places, center)

        self.assertEqual(print_mock.call_count, 1)
        warning = print_mock.call_args.args[0]
        self.assertIn("WARNING: tokyo-japan:beaumont-import", warning)
        self.assertIn("Kasama Restaurant", warning)

    def test_country_inference_keeps_monaco_english_with_localized_address_tails(self) -> None:
        raw = RawSavedList(
            title="Monaco 🇲🇨",
            places=[
                RawPlace(
                    name="Cafe",
                    address="42 Quai Jean-Charles Rey, 98000 Monaco, モナコ",
                    maps_url="https://maps.google.com/?cid=1",
                ),
                RawPlace(
                    name="Cathedral",
                    address="4 Rue Colonel Bellando de Castro, 98000 Monaco, モナコ",
                    maps_url="https://maps.google.com/?cid=2",
                ),
                RawPlace(
                    name="Garden",
                    address="モナコ 〒98000 モナコ",
                    maps_url="https://maps.google.com/?cid=3",
                ),
            ],
        )

        self.assertEqual(build_data.infer_country_name(raw.title or "", raw), "Monaco")
        self.assertEqual(
            build_data.infer_country_from_address("42 Quai Jean-Charles Rey, 98000 Monaco, モナコ"),
            "Monaco",
        )
        self.assertEqual(build_data.infer_country_code("Monaco"), "MC")

    def test_country_inference_uses_flag_before_address_fragments(self) -> None:
        raw = RawSavedList(
            title="Okinawa Main Island 🇯🇵",
            description="Michael's list for Okinawa, Japan 🇯🇵",
            places=[
                RawPlace(
                    name="Street Fragment",
                    address="〒900-0015 Okinawa, Naha, Kumoji, 2 Chome−19−17 RS-ONE 1F",
                    maps_url="https://maps.google.com/?cid=1",
                ),
                RawPlace(
                    name="Postal Fragment",
                    address="3 Chome-9-26 Makishi, Naha, Okinawa 900-0013",
                    maps_url="https://maps.google.com/?cid=2",
                ),
            ],
        )

        self.assertEqual(build_data.infer_country_name(raw.title or "", raw), "Japan")
        self.assertIsNone(build_data.infer_country_from_address(raw.places[0].address))
        self.assertIsNone(build_data.infer_country_from_address(raw.places[1].address))

    def test_address_locality_tags_exclude_buildings_blocks_and_postal_fragments(self) -> None:
        enrichment = EnrichmentPlace()
        addresses = [
            (
                "5 Chome-13-14 Jingumae, Shibuya City, Tokyo 150-0001, Japan",
                "Tokyo",
                {"jingumae", "shibuya-city", "tokyo"},
                {"chome-13-14-jingumae", "tokyo-150-0001"},
                "Jingumae",
            ),
            (
                "〒104-0045 Tokyo, Chuo City, Tsukiji, 3 Chome−16−9 アーバンメイツビル １F",
                "Tokyo",
                {"chuo-city", "tsukiji", "tokyo"},
                {"tokyo-104-0045", "chome-16-9", "urbanmeitsubiru"},
                "Tsukiji",
            ),
            (
                "Japan, 〒105-0004 Tokyo, Minato City, Shinbashi, 3 Chome−8−5, Le Gratteciel, 号 B1",
                "Tokyo",
                {"minato-city", "shinbashi", "tokyo"},
                {"le-gratteciel", "chome-8-5"},
                "Shinbashi",
            ),
            (
                "123 Main St, Williamsburg, Brooklyn, United States",
                "New York",
                {"williamsburg", "brooklyn", "new-york"},
                {"main-st", "united-states"},
                "Williamsburg",
            ),
            (
                "10 Rue de Rivoli, Le Marais, Paris, France",
                "Paris",
                {"le-marais", "paris"},
                {"rue-de-rivoli", "france"},
                "Le Marais",
            ),
            (
                "1 Via Roma, Brera, Milan, Italy",
                "Milan",
                {"brera", "milan"},
                {"via-roma", "italy"},
                "Brera",
            ),
            (
                "1 Oxford St, Soho, London, United Kingdom",
                "London",
                {"soho", "london"},
                {"oxford-st", "united-kingdom"},
                "Soho",
            ),
        ]

        for address, city_name, expected_tags, rejected_tags, expected_neighborhood in addresses:
            with self.subTest(address=address):
                place = RawPlace(
                    name="Test Place",
                    address=address,
                    maps_url="https://maps.google.com/?cid=1",
                )
                tags = set(
                    build_data.derive_place_tags(
                        place,
                        city_name,
                        enrichment=enrichment,
                        category=None,
                    )
                )

                self.assertEqual(
                    build_data.infer_neighborhood(address, city_name=city_name),
                    expected_neighborhood,
                )
                self.assertTrue(expected_tags.issubset(tags))
                self.assertTrue(tags.isdisjoint(rejected_tags))

    def test_import_saved_list_csv_reads_description_notes_and_maps_tokens(self) -> None:
        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "alishan.csv"
            csv_path.write_text(
                "\n".join(
                    [
                        "A foggy mountain weekend",
                        "",
                        "Title,Note,URL,Tags,Comment",
                        (
                            "Legacy Tea Stop,Best at sunrise,"
                            "https://www.google.com/maps/place/Tea+House/data=!4m2!3m1!1s0xabc123:0xdef456,,Order the oolong"
                        ),
                        "Fallback Name,,https://maps.app.goo.gl/short-link,,",
                    ]
                ),
                encoding="utf-8",
            )
            source = SourceConfig(
                slug="alishan-taiwan",
                type="google_export_csv",
                path=str(csv_path),
                title="Alishan, Taiwan",
            )

            saved_list = build_data.import_saved_list_csv(source)

        self.assertEqual(saved_list.title, "Alishan, Taiwan")
        self.assertEqual(saved_list.description, "A foggy mountain weekend")
        self.assertEqual(len(saved_list.places), 2)

        first_place = saved_list.places[0]
        second_place = saved_list.places[1]

        self.assertEqual(first_place.name, "Tea House")
        self.assertEqual(first_place.note, "Best at sunrise\n\nOrder the oolong")
        self.assertEqual(first_place.maps_place_token, "0xabc123:0xdef456")
        self.assertEqual(
            build_data.stable_place_id(first_place, source_type="google_export_csv"),
            "gms:0xabc123:0xdef456",
        )
        self.assertEqual(second_place.name, "Fallback Name")
        self.assertTrue(
            build_data.stable_place_id(second_place, source_type="google_export_csv").startswith("url:")
        )

    def test_normalize_guide_prefers_enrichment_name_for_csv_sources_and_tracks_provenance(self) -> None:
        raw = RawSavedList(
            title="Taipei, Taiwan",
            configured_source_type="google_export_csv",
            fetched_at="2026-04-15T00:00:00+00:00",
            refresh_after="2026-04-29T00:00:00+00:00",
            places=[
                RawPlace(
                    name="Legacy Tea Stop",
                    address=None,
                    maps_url="https://maps.google.com/?cid=1",
                    maps_place_token="0xabc123:0xdef456",
                )
            ],
        )
        place_id = build_data.stable_place_id(raw.places[0], source_type=raw.configured_source_type)
        enrichment_cache = {
            place_id: EnrichmentCacheEntry(
                fetched_at="2026-04-16T00:00:00+00:00",
                refresh_after="2026-04-23T00:00:00+00:00",
                query="Legacy Tea Stop",
                matched=True,
                score=80,
                place=EnrichmentPlace(
                    display_name="Modern Tea House",
                    formatted_address="1 Songshan, Taipei, Taiwan",
                    google_maps_uri="https://maps.google.com/?cid=override",
                    primary_type_display_name="Tea house",
                    types=["tea_house"],
                ),
            )
        }

        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            list_overrides_dir = tmpdir_path / "lists"
            place_overrides_dir = tmpdir_path / "places"
            list_overrides_dir.mkdir()
            place_overrides_dir.mkdir()

            with (
                patch.object(build_data, "LIST_OVERRIDES_DIR", list_overrides_dir),
                patch.object(build_data, "PLACE_OVERRIDES_DIR", place_overrides_dir),
            ):
                guide = build_data.normalize_guide(
                    "taipei-taiwan",
                    raw,
                    enrichment_cache=enrichment_cache,
                )

        place = guide.places[0]
        self.assertEqual(place.name, "Modern Tea House")
        self.assertEqual(place.address, "1 Songshan, Taipei, Taiwan")
        self.assertEqual(place.maps_url, "https://maps.google.com/?cid=override")
        self.assertEqual(place.provenance.name.source, "google_places")
        self.assertEqual(place.provenance.name.fetched_at, "2026-04-16T00:00:00+00:00")
        self.assertEqual(place.provenance.address.source, "google_places")
        self.assertEqual(place.provenance.maps_url.source, "google_places")
        self.assertEqual(place.provenance.primary_category.source, "google_places")
        self.assertEqual(
            {field.value: field.source for field in place.provenance.tags},
            {
                "taipei": "google_list",
                "tea-house": "google_places",
            },
        )

    def test_resolve_refresh_sources_matches_csv_path_selector(self) -> None:
        sources = [
            SourceConfig(
                slug="tokyo-japan",
                type="google_list_url",
                url="https://maps.app.goo.gl/tokyo",
            ),
            SourceConfig(
                slug="alishan-taiwan",
                type="google_export_csv",
                path="data/raw/alishan-taiwan.csv",
                title="Alishan, Taiwan",
            ),
        ]

        selected_sources = build_data.resolve_refresh_sources(sources, ["data/raw/alishan-taiwan.csv"])

        self.assertEqual([source.slug for source in selected_sources], ["alishan-taiwan"])

    def test_raw_source_refresh_reason_respects_future_refresh_after(self) -> None:
        source = SourceConfig(
            slug="tokyo-japan",
            type="google_list_url",
            url="https://maps.app.goo.gl/tokyo",
        )
        saved_list = RawSavedList(
            fetched_at=datetime.now(UTC).isoformat(),
            refresh_after=(datetime.now(UTC) + timedelta(days=1)).isoformat(),
            source_signature=build_data.raw_source_signature(source),
        )

        self.assertIsNone(build_data.raw_source_refresh_reason(source, saved_list))

    def test_raw_source_refresh_reason_detects_config_change_despite_future_refresh_after(self) -> None:
        source = SourceConfig(
            slug="tokyo-japan",
            type="google_list_url",
            url="https://maps.app.goo.gl/tokyo",
        )
        saved_list = RawSavedList(
            fetched_at=datetime.now(UTC).isoformat(),
            refresh_after=(datetime.now(UTC) + timedelta(days=1)).isoformat(),
            source_signature="old-signature",
        )

        self.assertEqual(build_data.raw_source_refresh_reason(source, saved_list), "source-config-changed")

    def test_raw_source_refresh_reason_accepts_legacy_url_source_signature(self) -> None:
        source = SourceConfig(
            slug="tokyo-japan",
            type="google_list_url",
            url="https://maps.app.goo.gl/tokyo",
        )
        saved_list = RawSavedList(
            fetched_at=datetime.now(UTC).isoformat(),
            refresh_after=(datetime.now(UTC) + timedelta(days=1)).isoformat(),
            source_signature=build_data.legacy_google_list_source_signature(source),
        )

        self.assertIsNone(build_data.raw_source_refresh_reason(source, saved_list))

    def test_refresh_raw_sources_skips_fresh_url_snapshot(self) -> None:
        source = SourceConfig(
            slug="tokyo-japan",
            type="google_list_url",
            url="https://maps.app.goo.gl/tokyo",
        )

        with TemporaryDirectory() as tmpdir:
            raw_dir = Path(tmpdir)
            raw_path = raw_dir / "tokyo-japan.json"
            build_data.write_json(
                raw_path,
                RawSavedList(
                    fetched_at=datetime.now(UTC).isoformat(),
                    refresh_after=(datetime.now(UTC) + timedelta(days=1)).isoformat(),
                    source_signature=build_data.raw_source_signature(source),
                    places=[],
                ),
            )

            with (
                patch.object(build_data, "RAW_DIR", raw_dir),
                patch.object(build_data, "load_sources", return_value=[source]),
                patch.object(build_data, "scrape_google_list_url") as scrape,
            ):
                build_data.refresh_raw_sources(
                    headed=False,
                    force_refresh=False,
                    refresh_lists=[],
                    refresh_workers=1,
                    refresh_startup_jitter_seconds=0,
                )

        scrape.assert_not_called()

    def test_refresh_raw_sources_force_bypasses_fresh_url_snapshot(self) -> None:
        source = SourceConfig(
            slug="tokyo-japan",
            type="google_list_url",
            url="https://maps.app.goo.gl/tokyo",
        )

        with TemporaryDirectory() as tmpdir:
            raw_dir = Path(tmpdir)
            raw_path = raw_dir / "tokyo-japan.json"
            build_data.write_json(
                raw_path,
                RawSavedList(
                    fetched_at=datetime.now(UTC).isoformat(),
                    refresh_after=(datetime.now(UTC) + timedelta(days=1)).isoformat(),
                    source_signature=build_data.raw_source_signature(source),
                    places=[],
                ),
            )

            with (
                patch.object(build_data, "RAW_DIR", raw_dir),
                patch.object(build_data, "load_sources", return_value=[source]),
                patch.object(
                    build_data,
                    "scrape_google_list_url",
                    return_value=RawSavedList(title="Fresh", places=[]),
                ) as scrape,
            ):
                build_data.refresh_raw_sources(
                    headed=False,
                    force_refresh=True,
                    refresh_lists=[],
                    refresh_workers=1,
                    refresh_startup_jitter_seconds=0,
                )

        scrape.assert_called_once_with(source, headed=False)

    def test_scraper_session_identity_key_changes_with_proxy(self) -> None:
        self.assertEqual(build_data.scraper_session_identity_key(None), "direct")
        self.assertNotEqual(
            build_data.scraper_session_identity_key("http://proxy-a.example:8080"),
            build_data.scraper_session_identity_key("http://proxy-b.example:8080"),
        )

    def test_ensure_scraper_session_state_expires_idle_sessions(self) -> None:
        with TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            stale_now = datetime(2026, 4, 1, tzinfo=UTC)
            fresh_now = stale_now + build_data.SCRAPER_SESSION_MAX_AGE + timedelta(minutes=1)

            with patch.object(build_data, "SCRAPER_STATE_DIR", state_dir):
                state = build_data.ensure_scraper_session_state(None, now=stale_now)
                state.browser_profile_dir.mkdir(parents=True, exist_ok=True)
                state.http_cookie_jar_path.write_text("cookie", encoding="utf-8")
                build_data.record_scraper_session_use(state, proxy=None, now=stale_now)

                refreshed = build_data.ensure_scraper_session_state(None, now=fresh_now)

            self.assertTrue(refreshed.identity_dir.is_dir())
            self.assertFalse(refreshed.http_cookie_jar_path.exists())
            self.assertFalse(refreshed.browser_profile_dir.exists())

    def test_build_scraper_session_state_uses_per_process_state_files(self) -> None:
        state = build_data.build_scraper_session_state("http://proxy.example:8080")
        pid_suffix = f"pid-{os.getpid()}"

        self.assertIn(pid_suffix, state.browser_profile_dir.name)
        self.assertIn(pid_suffix, state.http_cookie_jar_path.name)
        self.assertIn(pid_suffix, state.metadata_path.name)

    def test_clear_scraper_session_state_keeps_other_worker_files(self) -> None:
        with TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            with patch.object(build_data, "SCRAPER_STATE_DIR", state_dir):
                state = build_data.build_scraper_session_state(None)
                sibling_browser_dir = state.identity_dir / "browser" / "pid-99999"
                sibling_cookie_jar_path = state.identity_dir / "http-cookies.pid-99999.txt"
                sibling_metadata_path = state.identity_dir / "metadata.pid-99999.json"

                state.browser_profile_dir.mkdir(parents=True, exist_ok=True)
                state.http_cookie_jar_path.write_text("current", encoding="utf-8")
                state.metadata_path.write_text("{}", encoding="utf-8")

                sibling_browser_dir.mkdir(parents=True, exist_ok=True)
                sibling_cookie_jar_path.write_text("sibling", encoding="utf-8")
                sibling_metadata_path.write_text("{}", encoding="utf-8")

                build_data.clear_scraper_session_state(state)

            self.assertFalse(state.browser_profile_dir.exists())
            self.assertFalse(state.http_cookie_jar_path.exists())
            self.assertFalse(state.metadata_path.exists())
            self.assertTrue(sibling_browser_dir.exists())
            self.assertTrue(sibling_cookie_jar_path.exists())
            self.assertTrue(sibling_metadata_path.exists())

    def test_scrape_google_list_url_passes_persistent_sessions_to_scraper(self) -> None:
        source = SourceConfig(
            slug="tokyo-japan",
            type="google_list_url",
            url="https://maps.app.goo.gl/tokyo",
        )

        class DummySavedListResult:
            def to_dict(self) -> dict[str, object]:
                return {
                    "source_url": source.url,
                    "resolved_url": source.url,
                    "list_id": "tokyo",
                    "title": "Tokyo",
                    "description": None,
                    "places": [],
                }

        with TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            with (
                patch.object(build_data, "SCRAPER_STATE_DIR", state_dir),
                patch.dict("os.environ", {"GMAPS_SCRAPER_PROXY": "http://proxy.example:8080"}, clear=False),
                patch.object(build_data, "scrape_saved_list", return_value=DummySavedListResult()) as scrape,
            ):
                payload = build_data.scrape_google_list_url(source, headed=False)

        self.assertEqual(payload.title, "Tokyo")
        kwargs = scrape.call_args.kwargs
        self.assertTrue(kwargs["headless"])
        self.assertEqual(kwargs["browser_session"].proxy, "http://proxy.example:8080")
        self.assertEqual(kwargs["http_session"].proxy, "http://proxy.example:8080")
        self.assertIn("proxy-", str(kwargs["browser_session"].profile_dir))
        self.assertIn("proxy-", str(kwargs["http_session"].cookie_jar_path))

    def test_scrape_google_list_url_clears_session_after_block_and_retries(self) -> None:
        source = SourceConfig(
            slug="tokyo-japan",
            type="google_list_url",
            url="https://maps.app.goo.gl/tokyo",
        )

        class DummySavedListResult:
            def to_dict(self) -> dict[str, object]:
                return {
                    "source_url": source.url,
                    "resolved_url": source.url,
                    "list_id": "tokyo",
                    "title": "Tokyo",
                    "description": None,
                    "places": [],
                }

        with TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            with (
                patch.object(build_data, "SCRAPER_STATE_DIR", state_dir),
                patch.object(
                    build_data,
                    "scrape_saved_list",
                    side_effect=[
                        build_data.ScrapeError("HTTP 429 Too Many Requests"),
                        DummySavedListResult(),
                    ],
                ) as scrape,
                patch.object(
                    build_data,
                    "clear_scraper_session_state",
                    wraps=build_data.clear_scraper_session_state,
                ) as clear_state,
            ):
                payload = build_data.scrape_google_list_url(source, headed=False)

        self.assertEqual(payload.title, "Tokyo")
        self.assertEqual(scrape.call_count, 2)
        clear_state.assert_called_once()

    def test_fetch_places_enrichment_uses_place_page_without_api_key(self) -> None:
        place = RawPlace(
            name="Jimbocho Den",
            address="Tokyo, Japan",
            maps_url="https://www.google.com/maps/place/Jimbocho+Den",
        )
        details = SimpleNamespace(
            source_url=place.maps_url,
            resolved_url="https://www.google.com/maps/place/Jimbocho+Den/@35.67,139.71,17z",
            name="Jimbocho Den",
            category="Japanese restaurant",
            rating=4.8,
            review_count=324,
            address="Tokyo, Japan",
            status="Closed · Opens 6 PM",
            website="http://www.jimbochoden.com/",
            phone="+81 3-6455-5433",
            plus_code="MPF7+73 Shibuya, Tokyo, Japan",
            description="Modern kaiseki.",
            limited_view=False,
        )

        with patch.object(build_data, "scrape_place", return_value=details) as scrape:
            entry = build_data.fetch_places_enrichment(place, api_key=None)

        scrape.assert_called_once()
        self.assertEqual(entry.source, "google_maps_page")
        self.assertTrue(entry.matched)
        self.assertEqual(entry.place.display_name, "Jimbocho Den")
        self.assertEqual(entry.place.primary_type, "japanese_restaurant")
        self.assertEqual(entry.place.user_rating_count, 324)
        self.assertEqual(entry.place.business_status, "OPERATIONAL")

    def test_fetch_places_enrichment_falls_back_to_api_when_page_is_limited(self) -> None:
        place = RawPlace(
            name="Jimbocho Den",
            address="Tokyo, Japan",
            maps_url="https://www.google.com/maps/place/Jimbocho+Den",
        )
        page_entry = EnrichmentCacheEntry(
            fetched_at="2026-04-16T00:00:00+00:00",
            refresh_after="2026-04-19T00:00:00+00:00",
            source="google_maps_page",
            query="Jimbocho Den, Tokyo, Japan",
            matched=True,
            score=45,
            place=EnrichmentPlace(
                display_name="Jimbocho Den",
                formatted_address="Tokyo, Japan",
                google_maps_uri=place.maps_url,
                limited_view=True,
            ),
        )
        api_entry = EnrichmentCacheEntry(
            fetched_at="2026-04-16T00:00:00+00:00",
            refresh_after="2026-04-23T00:00:00+00:00",
            source="google_places_api",
            query="Jimbocho Den, Tokyo, Japan",
            matched=True,
            score=88,
            place=EnrichmentPlace(
                display_name="Jimbocho Den",
                formatted_address="Tokyo, Japan",
                google_maps_uri="https://maps.google.com/?cid=1",
                primary_type="restaurant",
                primary_type_display_name="Restaurant",
            ),
        )

        with (
            patch.object(build_data, "fetch_place_page_enrichment", return_value=page_entry) as page_fetch,
            patch.object(build_data, "fetch_places_api_enrichment", return_value=api_entry) as api_fetch,
        ):
            entry = build_data.fetch_places_enrichment(place, api_key="test-key")

        page_fetch.assert_called_once_with(place)
        api_fetch.assert_called_once_with(place, api_key="test-key")
        self.assertIs(entry, api_entry)

    def test_parallel_refresh_writes_each_snapshot_as_it_finishes(self) -> None:
        first_source = SourceConfig(
            slug="tokyo-japan",
            type="google_list_url",
            url="https://maps.app.goo.gl/tokyo",
        )
        second_source = SourceConfig(
            slug="madrid-spain",
            type="google_list_url",
            url="https://maps.app.goo.gl/madrid",
        )

        with TemporaryDirectory() as tmpdir:
            raw_dir = Path(tmpdir)
            first_raw_path = raw_dir / "tokyo-japan.json"

            class FakeFuture:
                def __init__(self, result):
                    self._result = result

                def result(self):
                    return self._result()

            first_future = FakeFuture(lambda: RawSavedList(title="Tokyo", places=[]))

            def second_result() -> RawSavedList:
                self.assertTrue(first_raw_path.exists())
                return RawSavedList(title="Madrid", places=[])

            second_future = FakeFuture(second_result)
            futures_by_slug = {
                first_source.slug: first_future,
                second_source.slug: second_future,
            }
            test_case = self

            class FakeExecutor:
                def __init__(self, max_workers):
                    self.max_workers = max_workers

                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    return False

                def submit(self, _fn, source, **kwargs):
                    test_case.assertEqual(kwargs["refresh_retries"], build_data.DEFAULT_REFRESH_RETRIES)
                    test_case.assertFalse(kwargs["headed"])
                    return futures_by_slug[source.slug]

            with (
                patch.object(build_data, "RAW_DIR", raw_dir),
                patch.object(build_data, "load_sources", return_value=[first_source, second_source]),
                patch.object(build_data, "ProcessPoolExecutor", FakeExecutor),
                patch.object(build_data, "as_completed", return_value=[first_future, second_future]),
            ):
                build_data.refresh_raw_sources(
                    headed=False,
                    force_refresh=True,
                    refresh_lists=[],
                    refresh_workers=2,
                    refresh_startup_jitter_seconds=0,
                )

            first_payload = RawSavedList.model_validate_json(first_raw_path.read_text(encoding="utf-8"))
            second_payload = RawSavedList.model_validate_json(
                (raw_dir / "madrid-spain.json").read_text(encoding="utf-8")
            )

        self.assertEqual(first_payload.title, "Tokyo")
        self.assertEqual(second_payload.title, "Madrid")

    def test_refresh_retries_transient_scrape_failure(self) -> None:
        source = SourceConfig(
            slug="tokyo-japan",
            type="google_list_url",
            url="https://maps.app.goo.gl/tokyo",
        )

        with (
            patch.object(build_data, "sleep_for_refresh_startup_jitter") as startup_sleep,
            patch.object(build_data.time, "sleep") as retry_sleep,
            patch.object(
                build_data,
                "scrape_google_list_url",
                side_effect=[build_data.ScrapeError("timeout"), RawSavedList(title="Tokyo", places=[])],
            ) as scrape,
        ):
            payload = build_data.scrape_google_list_url_with_retries(
                source,
                headed=False,
                refresh_retries=2,
                refresh_retry_backoff_seconds=10,
                refresh_startup_jitter_seconds=8,
            )

        self.assertEqual(payload.title, "Tokyo")
        self.assertEqual(scrape.call_count, 2)
        self.assertEqual(startup_sleep.call_count, 2)
        retry_sleep.assert_called_once_with(10)

    def test_refresh_retries_transient_parse_failure(self) -> None:
        source = SourceConfig(
            slug="tokyo-japan",
            type="google_list_url",
            url="https://maps.app.goo.gl/tokyo",
        )

        with (
            patch.object(build_data, "sleep_for_refresh_startup_jitter"),
            patch.object(build_data.time, "sleep") as retry_sleep,
            patch.object(
                build_data,
                "scrape_google_list_url",
                side_effect=[build_data.ParseError("missing list payload"), RawSavedList(title="Tokyo", places=[])],
            ) as scrape,
        ):
            payload = build_data.scrape_google_list_url_with_retries(
                source,
                headed=False,
                refresh_retries=2,
                refresh_retry_backoff_seconds=10,
                refresh_startup_jitter_seconds=8,
            )

        self.assertEqual(payload.title, "Tokyo")
        self.assertEqual(scrape.call_count, 2)
        retry_sleep.assert_called_once_with(10)

    def test_refresh_raw_sources_keeps_existing_snapshot_when_refresh_fails(self) -> None:
        source = SourceConfig(
            slug="tokyo-japan",
            type="google_list_url",
            url="https://maps.app.goo.gl/tokyo",
        )

        with TemporaryDirectory() as tmpdir:
            raw_dir = Path(tmpdir)
            raw_path = raw_dir / "tokyo-japan.json"
            build_data.write_json(
                raw_path,
                RawSavedList(
                    title="Backup",
                    fetched_at=datetime.now(UTC).isoformat(),
                    refresh_after=(datetime.now(UTC) - timedelta(days=1)).isoformat(),
                    source_signature=build_data.raw_source_signature(source),
                    places=[],
                ),
            )

            with (
                patch.object(build_data, "RAW_DIR", raw_dir),
                patch.object(build_data, "load_sources", return_value=[source]),
                patch.object(build_data, "scrape_google_list_url", side_effect=build_data.ScrapeError("timeout")),
            ):
                build_data.refresh_raw_sources(
                    headed=False,
                    force_refresh=False,
                    refresh_lists=[],
                    refresh_workers=1,
                    refresh_retries=0,
                    refresh_startup_jitter_seconds=0,
                )

            payload = RawSavedList.model_validate_json(raw_path.read_text(encoding="utf-8"))

        self.assertEqual(payload.title, "Backup")

    def test_refresh_raw_sources_keeps_existing_snapshot_when_parse_fails(self) -> None:
        source = SourceConfig(
            slug="tokyo-japan",
            type="google_list_url",
            url="https://maps.app.goo.gl/tokyo",
        )

        with TemporaryDirectory() as tmpdir:
            raw_dir = Path(tmpdir)
            raw_path = raw_dir / "tokyo-japan.json"
            build_data.write_json(
                raw_path,
                RawSavedList(
                    title="Backup",
                    fetched_at=datetime.now(UTC).isoformat(),
                    refresh_after=(datetime.now(UTC) - timedelta(days=1)).isoformat(),
                    source_signature=build_data.raw_source_signature(source),
                    places=[],
                ),
            )

            with (
                patch.object(build_data, "RAW_DIR", raw_dir),
                patch.object(build_data, "load_sources", return_value=[source]),
                patch.object(
                    build_data,
                    "scrape_google_list_url",
                    side_effect=build_data.ParseError("missing list payload"),
                ),
            ):
                build_data.refresh_raw_sources(
                    headed=False,
                    force_refresh=False,
                    refresh_lists=[],
                    refresh_workers=1,
                    refresh_retries=0,
                    refresh_startup_jitter_seconds=0,
                )

            payload = RawSavedList.model_validate_json(raw_path.read_text(encoding="utf-8"))

        self.assertEqual(payload.title, "Backup")

    def test_refresh_raw_sources_does_not_hide_unexpected_refresh_errors(self) -> None:
        source = SourceConfig(
            slug="tokyo-japan",
            type="google_list_url",
            url="https://maps.app.goo.gl/tokyo",
        )

        with TemporaryDirectory() as tmpdir:
            raw_dir = Path(tmpdir)
            raw_path = raw_dir / "tokyo-japan.json"
            build_data.write_json(
                raw_path,
                RawSavedList(
                    title="Backup",
                    fetched_at=datetime.now(UTC).isoformat(),
                    refresh_after=(datetime.now(UTC) - timedelta(days=1)).isoformat(),
                    source_signature=build_data.raw_source_signature(source),
                    places=[],
                ),
            )

            with (
                patch.object(build_data, "RAW_DIR", raw_dir),
                patch.object(build_data, "load_sources", return_value=[source]),
                patch.object(build_data, "scrape_google_list_url", side_effect=ValueError("bad data")),
            ):
                with self.assertRaisesRegex(ValueError, "bad data"):
                    build_data.refresh_raw_sources(
                        headed=False,
                        force_refresh=False,
                        refresh_lists=[],
                        refresh_workers=1,
                        refresh_retries=0,
                        refresh_startup_jitter_seconds=0,
                    )

            payload = RawSavedList.model_validate_json(raw_path.read_text(encoding="utf-8"))

        self.assertEqual(payload.title, "Backup")

    def test_refresh_raw_sources_rejects_backup_when_source_config_changed(self) -> None:
        current_source = SourceConfig(
            slug="tokyo-japan",
            type="google_list_url",
            url="https://maps.app.goo.gl/current",
        )
        previous_source = SourceConfig(
            slug="tokyo-japan",
            type="google_list_url",
            url="https://maps.app.goo.gl/previous",
        )

        with TemporaryDirectory() as tmpdir:
            raw_dir = Path(tmpdir)
            raw_path = raw_dir / "tokyo-japan.json"
            build_data.write_json(
                raw_path,
                RawSavedList(
                    title="Previous List",
                    fetched_at=datetime.now(UTC).isoformat(),
                    refresh_after=(datetime.now(UTC) + timedelta(days=1)).isoformat(),
                    source_signature=build_data.raw_source_signature(previous_source),
                    places=[],
                ),
            )

            with (
                patch.object(build_data, "RAW_DIR", raw_dir),
                patch.object(build_data, "load_sources", return_value=[current_source]),
                patch.object(build_data, "scrape_google_list_url", side_effect=build_data.ScrapeError("timeout")),
            ):
                with self.assertRaisesRegex(RuntimeError, "Raw refresh failed for 1 source"):
                    build_data.refresh_raw_sources(
                        headed=False,
                        force_refresh=False,
                        refresh_lists=[],
                        refresh_workers=1,
                        refresh_retries=0,
                        refresh_startup_jitter_seconds=0,
                    )

            payload = RawSavedList.model_validate_json(raw_path.read_text(encoding="utf-8"))

        self.assertEqual(payload.title, "Previous List")

    def test_google_list_refresh_after_uses_stable_source_jitter(self) -> None:
        source = SourceConfig(
            slug="tokyo-japan",
            type="google_list_url",
            url="https://maps.app.goo.gl/tokyo",
        )
        fetched_at = datetime(2026, 4, 15, tzinfo=UTC)
        source_signature = build_data.raw_source_signature(source)

        refresh_after = build_data.raw_source_refresh_after(
            fetched_at,
            source,
            source_signature=source_signature,
        )
        repeated_refresh_after = build_data.raw_source_refresh_after(
            fetched_at,
            source,
            source_signature=source_signature,
        )
        lower_bound = fetched_at + build_data.RAW_SOURCE_CACHE_TTL - build_data.RAW_SOURCE_REFRESH_JITTER
        upper_bound = fetched_at + build_data.RAW_SOURCE_CACHE_TTL + build_data.RAW_SOURCE_REFRESH_JITTER

        self.assertEqual(refresh_after, repeated_refresh_after)
        self.assertGreaterEqual(refresh_after, lower_bound)
        self.assertLessEqual(refresh_after, upper_bound)


if __name__ == "__main__":
    unittest.main()
