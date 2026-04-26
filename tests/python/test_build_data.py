from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import UTC, datetime, timedelta
from io import BytesIO, StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from pydantic import ValidationError
from PIL import Image

from scripts import build_data
from scripts.pipeline_models import (
    EnrichmentCacheEntry,
    EnrichmentPlace,
    Guide,
    NormalizedPlace,
    RawPlace,
    RawSavedList,
    SourceConfig,
)


class BuildDataTests(unittest.TestCase):
    def test_format_duration_seconds_formats_short_and_long_values(self) -> None:
        self.assertEqual(build_data.format_duration_seconds(9.34), "9.3s")
        self.assertEqual(build_data.format_duration_seconds(68.25), "1m 08.3s")
        self.assertEqual(build_data.format_duration_seconds(59.95), "1m 00.0s")

    def test_default_refresh_workers_scales_down_to_cpu_count(self) -> None:
        with patch.object(build_data.os, "cpu_count", return_value=2):
            self.assertEqual(build_data.default_refresh_workers(), 2)

    def test_default_refresh_workers_caps_at_four(self) -> None:
        with patch.object(build_data.os, "cpu_count", return_value=16):
            self.assertEqual(build_data.default_refresh_workers(), 4)

    def test_parser_uses_auto_refresh_worker_default(self) -> None:
        parser = build_data.build_parser()
        args = parser.parse_args([])

        self.assertEqual(args.refresh_workers, build_data.DEFAULT_REFRESH_WORKERS)

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

    def test_preserve_existing_raw_saved_list_keeps_stronger_prior_place_fields(self) -> None:
        existing_payload = RawSavedList(
            configured_source_type="google_list_url",
            places=[
                RawPlace(
                    name="Duke's Waikiki",
                    address="2335 Kalākaua Ave #116, Honolulu, HI 96815, United States",
                    is_favorite=True,
                    lat=21.2769032,
                    lng=-157.8278887,
                    maps_url="https://www.google.com/maps/search/?api=1&query=Duke%27s+Waikiki",
                    cid="8935267511126082507",
                    google_id="/g/1tdwf48w",
                    maps_place_token="0xdeadbeef:0x1",
                )
            ],
        )
        refreshed_payload = RawSavedList(
            configured_source_type="google_list_url",
            places=[
                RawPlace(
                    name="Duke's Waikiki",
                    address=None,
                    is_favorite=False,
                    lat=21.2769032,
                    lng=-157.8278887,
                    maps_url="https://www.google.com/maps/search/?api=1&query=Duke%27s+Waikiki",
                    cid="8935267511126082507",
                    google_id=None,
                    maps_place_token=None,
                )
            ],
        )

        merged = build_data.preserve_existing_raw_saved_list(
            slug="oahu-hawaii-usa",
            existing_payload=existing_payload,
            refreshed_payload=refreshed_payload,
        )

        self.assertEqual(
            merged.places[0].address,
            "2335 Kalākaua Ave #116, Honolulu, HI 96815, United States",
        )
        self.assertEqual(merged.places[0].google_id, "/g/1tdwf48w")
        self.assertEqual(merged.places[0].maps_place_token, "0xdeadbeef:0x1")
        self.assertTrue(merged.places[0].is_favorite)

    def test_preserve_existing_raw_saved_list_does_not_apply_to_non_matching_place(self) -> None:
        existing_payload = RawSavedList(
            configured_source_type="google_list_url",
            places=[
                RawPlace(
                    name="Old Place",
                    address="1 Example St",
                    lat=35.0,
                    lng=139.0,
                    maps_url="https://www.google.com/maps/search/?api=1&query=Old+Place",
                    cid="111",
                    google_id="/g/old",
                )
            ],
        )
        refreshed_payload = RawSavedList(
            configured_source_type="google_list_url",
            places=[
                RawPlace(
                    name="New Place",
                    address=None,
                    lat=36.0,
                    lng=140.0,
                    maps_url="https://www.google.com/maps/search/?api=1&query=New+Place",
                    cid="222",
                    google_id=None,
                )
            ],
        )

        merged = build_data.preserve_existing_raw_saved_list(
            slug="tokyo-japan",
            existing_payload=existing_payload,
            refreshed_payload=refreshed_payload,
        )

        self.assertIsNone(merged.places[0].address)
        self.assertIsNone(merged.places[0].google_id)

    def test_preserve_existing_raw_saved_list_skips_fields_when_names_do_not_match(self) -> None:
        existing_payload = RawSavedList(
            configured_source_type="google_list_url",
            places=[
                RawPlace(
                    name="Michael's list for Seoul, Korea",
                    address="BONTÉ 본태",
                    lat=37.0,
                    lng=127.0,
                    maps_url="https://www.google.com/maps/search/?api=1&query=BONT%C3%89",
                    cid="333",
                )
            ],
        )
        refreshed_payload = RawSavedList(
            configured_source_type="google_list_url",
            places=[
                RawPlace(
                    name="BONTÉ 본태",
                    address=None,
                    lat=37.0,
                    lng=127.0,
                    maps_url="https://www.google.com/maps/search/?api=1&query=BONT%C3%89",
                    cid="333",
                )
            ],
        )

        merged = build_data.preserve_existing_raw_saved_list(
            slug="seoul-korea",
            existing_payload=existing_payload,
            refreshed_payload=refreshed_payload,
        )

        self.assertIsNone(merged.places[0].address)

    def test_preserve_existing_raw_saved_list_skips_note_like_address_restore(self) -> None:
        existing_payload = RawSavedList(
            configured_source_type="google_list_url",
            places=[
                RawPlace(
                    name="Gorat's Steakhouse",
                    address="Warren Buffet's favorite steakhouse",
                    note="Warren Buffet's favorite steakhouse",
                    lat=41.2412347,
                    lng=-95.9886973,
                    maps_url="https://www.google.com/maps/search/?api=1&query=Gorat%27s+Steakhouse",
                    cid="10697208099166303129",
                )
            ],
        )
        refreshed_payload = RawSavedList(
            configured_source_type="google_list_url",
            places=[
                RawPlace(
                    name="Gorat's Steakhouse",
                    address=None,
                    note="Warren Buffet's favorite steakhouse",
                    lat=41.2412347,
                    lng=-95.9886973,
                    maps_url="https://www.google.com/maps/search/?api=1&query=Gorat%27s+Steakhouse",
                    cid="10697208099166303129",
                )
            ],
        )

        merged = build_data.preserve_existing_raw_saved_list(
            slug="omaha-nebraska-usa",
            existing_payload=existing_payload,
            refreshed_payload=refreshed_payload,
        )

        self.assertIsNone(merged.places[0].address)

    def test_preserve_existing_raw_saved_list_skips_generic_locality_address_restore(self) -> None:
        existing_payload = RawSavedList(
            configured_source_type="google_list_url",
            places=[
                RawPlace(
                    name="青湾情人海灘",
                    address="台湾 澎湖縣馬公市嵵裡里",
                    lat=23.5319641,
                    lng=119.553573,
                    maps_url="https://www.google.com/maps/search/?api=1&query=%E9%9D%92%E6%B9%BE%E6%83%85%E4%BA%BA%E6%B5%B7%E7%81%98",
                    cid="3777482742712049795",
                    google_id="/g/11x9_8ngd",
                )
            ],
        )
        refreshed_payload = RawSavedList(
            configured_source_type="google_list_url",
            places=[
                RawPlace(
                    name="青湾情人海灘",
                    address=None,
                    lat=23.5319641,
                    lng=119.553573,
                    maps_url="https://www.google.com/maps/search/?api=1&query=%E9%9D%92%E6%B9%BE%E6%83%85%E4%BA%BA%E6%B5%B7%E7%81%98",
                    cid="3777482742712049795",
                    google_id="/g/11x9_8ngd",
                )
            ],
        )

        merged = build_data.preserve_existing_raw_saved_list(
            slug="penghu-taiwan",
            existing_payload=existing_payload,
            refreshed_payload=refreshed_payload,
        )

        self.assertIsNone(merged.places[0].address)
        self.assertEqual(merged.places[0].google_id, "/g/11x9_8ngd")

    def test_preserve_existing_raw_saved_list_skips_cross_source_signature_reuse(self) -> None:
        previous_source = SourceConfig(
            slug="los-angeles-california-usa",
            url="https://maps.app.goo.gl/old-list",
        )
        current_source = SourceConfig(
            slug="los-angeles-california-usa",
            url="https://maps.app.goo.gl/new-list",
        )
        existing_payload = RawSavedList(
            configured_source_type="google_list_url",
            source_signature=build_data.raw_source_signature(previous_source),
            places=[
                RawPlace(
                    name="Global Village",
                    address="38C5+F57 - Wadi Al Safa 4 - Dubai - United Arab Emirates",
                    is_favorite=True,
                    lat=25.0716887,
                    lng=55.3084347,
                    maps_url="https://www.google.com/maps/search/?api=1&query=Global+Village",
                    cid="4494433615859482133",
                    google_id="/g/11oldsource",
                    maps_place_token="0xold:0x1",
                )
            ],
        )
        refreshed_payload = RawSavedList(
            configured_source_type="google_list_url",
            source_signature=build_data.raw_source_signature(current_source),
            places=[
                RawPlace(
                    name="Global Village",
                    address=None,
                    is_favorite=False,
                    lat=25.0716887,
                    lng=55.3084347,
                    maps_url="https://www.google.com/maps/search/?api=1&query=Global+Village",
                    cid=None,
                    google_id=None,
                    maps_place_token=None,
                )
            ],
        )

        merged = build_data.preserve_existing_raw_saved_list(
            source=current_source,
            slug="los-angeles-california-usa",
            existing_payload=existing_payload,
            refreshed_payload=refreshed_payload,
        )

        self.assertIsNone(merged.places[0].address)
        self.assertIsNone(merged.places[0].google_id)
        self.assertIsNone(merged.places[0].cid)
        self.assertIsNone(merged.places[0].maps_place_token)
        self.assertFalse(merged.places[0].is_favorite)

    def test_build_place_page_candidate_urls_prefers_search_for_cid_inputs(self) -> None:
        place = RawPlace(
            name="Sister Midnight",
            address="4 Rue Viollet-le-Duc, 75009 Paris, France",
            maps_url="https://maps.google.com/?cid=5180951040094558101",
        )

        self.assertEqual(
            build_data.build_place_page_candidate_urls(place),
            [
                (
                    "https://www.google.com/maps/search/?api=1&query="
                    "Sister+Midnight%2C+4+Rue+Viollet-le-Duc%2C+75009+Paris%2C+France"
                    "&hl=en&gl=us"
                ),
                "https://maps.google.com/?cid=5180951040094558101&hl=en&gl=us",
            ],
        )

    def test_build_place_page_candidate_urls_adds_cid_fallback_for_search_urls(self) -> None:
        place = RawPlace(
            name="Swagat Wine & Dine",
            address="Bau St, Suva, Fiji",
            maps_url=(
                "https://www.google.com/maps/search/?api=1"
                "&query=Swagat+Wine+%26+Dine%2C+Bau+St%2C+Suva%2C+Fiji"
            ),
            cid="15264186070483398692",
        )

        self.assertEqual(
            build_data.build_place_page_candidate_urls(place),
            [
                (
                    "https://www.google.com/maps/search/?api=1"
                    "&query=Swagat+Wine+%26+Dine%2C+Bau+St%2C+Suva%2C+Fiji&hl=en&gl=us"
                ),
                "https://maps.google.com/?cid=15264186070483398692&hl=en&gl=us",
            ],
        )

    def test_build_place_page_candidate_urls_replaces_name_only_search_urls_with_city_country_bias(self) -> None:
        place = RawPlace(
            name="Locale",
            address=None,
            maps_url="https://www.google.com/maps/search/?api=1&query=Locale",
            cid="6924437521980544303",
            lat=35.636208,
            lng=139.709467,
        )

        self.assertEqual(
            build_data.build_place_page_candidate_urls(
                place,
                city_name="Tokyo",
                country_name="Japan",
            ),
            [
                (
                    "https://www.google.com/maps/search/?api=1"
                    "&query=Locale%2C+Tokyo%2C+Japan&hl=en&gl=us"
                ),
                "https://www.google.com/maps/search/?api=1&query=Locale&hl=en&gl=us",
                "https://maps.google.com/?cid=6924437521980544303&hl=en&gl=us",
            ],
        )

    def test_build_place_page_candidate_urls_keeps_fallbacks_when_google_place_id_is_present(self) -> None:
        place = RawPlace(
            name="Cantina OK!",
            address="Council Pl, Sydney NSW 2000, Australia",
            maps_url="https://maps.google.com/?cid=7715422616180689913",
            cid="7715422616180689913",
        )

        self.assertEqual(
            build_data.build_place_page_candidate_urls(
                place,
                google_place_id="ChIJGcmcg7ZC1moRAOacd3HoEwM",
            ),
            [
                (
                    "https://www.google.com/maps/search/?api=1"
                    "&query=Cantina+OK%21%2C+Council+Pl%2C+Sydney+NSW+2000%2C+Australia"
                    "&query_place_id=ChIJGcmcg7ZC1moRAOacd3HoEwM&hl=en&gl=us"
                ),
                (
                    "https://www.google.com/maps/search/?api=1"
                    "&query=Cantina+OK%21%2C+Council+Pl%2C+Sydney+NSW+2000%2C+Australia&hl=en&gl=us"
                ),
                "https://maps.google.com/?cid=7715422616180689913&hl=en&gl=us",
            ],
        )

    def test_localize_google_maps_scrape_url_forces_english_locale(self) -> None:
        self.assertEqual(
            build_data.localize_google_maps_scrape_url(
                "https://www.google.com/maps/place/Test+Place?entry=ttu&hl=ja"
            ),
            "https://www.google.com/maps/place/Test+Place?entry=ttu&hl=en&gl=us",
        )
        self.assertEqual(
            build_data.localize_google_maps_scrape_url("https://example.com/place/Test+Place?hl=ja"),
            "https://example.com/place/Test+Place?hl=ja",
        )

    def test_fetch_place_page_enrichment_prefers_search_candidate_for_cid_inputs(self) -> None:
        place = RawPlace(
            name="Sister Midnight",
            address="4 Rue Viollet-le-Duc, 75009 Paris, France",
            maps_url="https://maps.google.com/?cid=5180951040094558101",
            lat=48.8814703,
            lng=2.340862,
        )
        called_urls: list[str] = []

        def fake_scrape_place(url: str, **_: object) -> SimpleNamespace:
            called_urls.append(url)
            return SimpleNamespace(
                source_url=url,
                resolved_url=(
                    "https://www.google.com/maps/place/Sister+Midnight/"
                    "@48.8814703,2.340862,17z/data=!3m1!4b1"
                ),
                name="Sister Midnight",
                category="Cocktail bar",
                rating=4.7,
                review_count=321,
                address="4 Rue Viollet-le-Duc, 75009 Paris, France",
                located_in=None,
                status=None,
                website="https://sistermidnightparis.com/",
                phone="+33 1 42 00 00 00",
                plus_code=None,
                description=None,
                limited_view=False,
            )

        with (
            patch.object(build_data, "scrape_place", side_effect=fake_scrape_place),
            patch.object(
                build_data,
                "build_scraper_sessions",
                return_value=(SimpleNamespace(), None, None),
            ),
            patch.object(build_data, "record_scraper_session_use"),
            patch.object(build_data, "release_scraper_session_lock"),
        ):
            entry = build_data.fetch_place_page_enrichment(place)

        self.assertEqual(
            called_urls,
            [
                (
                    "https://www.google.com/maps/search/?api=1&query="
                    "Sister+Midnight%2C+4+Rue+Viollet-le-Duc%2C+75009+Paris%2C+France"
                    "&hl=en&gl=us"
                )
            ],
        )
        self.assertTrue(entry.matched)
        self.assertIsNotNone(entry.place)
        assert entry.place is not None
        self.assertEqual(entry.place.display_name, "Sister Midnight")
        self.assertEqual(entry.place.primary_type_display_name, "Cocktail bar")

    def test_fetch_place_page_enrichment_uses_city_country_bias_for_name_only_search_urls(self) -> None:
        place = RawPlace(
            name="Locale",
            address=None,
            maps_url="https://www.google.com/maps/search/?api=1&query=Locale",
            cid="6924437521980544303",
            lat=35.636208,
            lng=139.709467,
        )
        called_urls: list[str] = []

        def fake_scrape_place(url: str, **_: object) -> SimpleNamespace:
            called_urls.append(url)
            return SimpleNamespace(
                source_url=url,
                resolved_url="https://www.google.com/maps/place/Locale/@35.636208,139.709467,17z",
                name="Locale",
                category="Cafe",
                rating=4.4,
                review_count=215,
                address="1 Chome-19-14 Aobadai, Meguro City, Tokyo 153-0042, Japan",
                located_in=None,
                status=None,
                website="https://locale.jp/",
                phone="+81 3-1234-5678",
                plus_code=None,
                description=None,
                lat=35.636208,
                lng=139.709467,
                limited_view=False,
            )

        with (
            patch.object(build_data, "scrape_place", side_effect=fake_scrape_place),
            patch.object(
                build_data,
                "build_scraper_sessions",
                return_value=(SimpleNamespace(), None, None),
            ),
            patch.object(build_data, "record_scraper_session_use"),
            patch.object(build_data, "release_scraper_session_lock"),
        ):
            entry = build_data.fetch_place_page_enrichment(
                place,
                city_name="Tokyo",
                country_name="Japan",
            )

        self.assertEqual(
            called_urls,
            [
                (
                    "https://www.google.com/maps/search/?api=1"
                    "&query=Locale%2C+Tokyo%2C+Japan&hl=en&gl=us"
                )
            ],
        )
        self.assertTrue(entry.matched)
        self.assertIsNotNone(entry.place)
        self.assertEqual(entry.query, "Locale, Tokyo, Japan")

    def test_fetch_places_api_enrichment_uses_city_country_bias_for_name_only_search_urls(self) -> None:
        place = RawPlace(
            name="Bilmonte",
            address=None,
            maps_url="https://www.google.com/maps/search/?api=1&query=Bilmonte",
            cid="1343378048703211865",
            lat=41.3894089,
            lng=2.1636435,
        )
        captured_request_body: dict[str, object] = {}

        class FakeResponse:
            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                return None

            def read(self) -> bytes:
                return b'{"places":[]}'

        def fake_urlopen(request, timeout: int = 20):  # type: ignore[no-untyped-def]
            self.assertEqual(timeout, 20)
            captured_request_body.update(json.loads(request.data.decode("utf-8")))
            return FakeResponse()

        with patch.object(build_data, "urlopen", side_effect=fake_urlopen):
            entry = build_data.fetch_places_api_enrichment(
                place,
                city_name="Barcelona",
                country_name="Spain",
                api_key="test-key",
            )

        self.assertEqual(
            captured_request_body["textQuery"],
            "Bilmonte, Barcelona, Spain",
        )
        self.assertEqual(entry.query, "Bilmonte, Barcelona, Spain")
        self.assertFalse(entry.matched)

    def test_fetch_place_page_enrichment_falls_back_to_canonical_url_for_weak_search_match(self) -> None:
        place = RawPlace(
            name="Sister Midnight",
            address="4 Rue Viollet-le-Duc, 75009 Paris, France",
            maps_url="https://maps.google.com/?cid=5180951040094558101",
            lat=48.8814703,
            lng=2.340862,
        )
        called_urls: list[str] = []

        def fake_scrape_place(url: str, **_: object) -> SimpleNamespace:
            called_urls.append(url)
            if "/maps/search/" in url:
                return SimpleNamespace(
                    source_url=url,
                    resolved_url="https://www.google.com/maps/place/Another+Bar/@48.88,2.34,17z",
                    name="Another Bar",
                    category="Cocktail bar",
                    rating=4.7,
                    review_count=321,
                    address="1 Different Street, 75009 Paris, France",
                    located_in=None,
                    status=None,
                    website="https://wrong.example/",
                    phone="+33 1 42 00 00 00",
                    plus_code=None,
                    description=None,
                    lat=48.8905,
                    lng=2.3305,
                    limited_view=False,
                )
            return SimpleNamespace(
                source_url=url,
                resolved_url=(
                    "https://www.google.com/maps/place/Sister+Midnight/"
                    "@48.8814703,2.340862,17z/data=!3m1!4b1"
                ),
                name="Sister Midnight",
                category="Cocktail bar",
                rating=4.7,
                review_count=321,
                address="4 Rue Viollet-le-Duc, 75009 Paris, France",
                located_in=None,
                status=None,
                website="https://sistermidnightparis.com/",
                phone="+33 1 42 00 00 00",
                plus_code=None,
                description=None,
                lat=48.8814703,
                lng=2.340862,
                limited_view=False,
            )

        with (
            patch.object(build_data, "scrape_place", side_effect=fake_scrape_place),
            patch.object(
                build_data,
                "build_scraper_sessions",
                return_value=(SimpleNamespace(), None, None),
            ),
            patch.object(build_data, "record_scraper_session_use"),
            patch.object(build_data, "release_scraper_session_lock"),
        ):
            entry = build_data.fetch_place_page_enrichment(place)

        self.assertEqual(
            called_urls,
            [
                (
                    "https://www.google.com/maps/search/?api=1&query="
                    "Sister+Midnight%2C+4+Rue+Viollet-le-Duc%2C+75009+Paris%2C+France"
                    "&hl=en&gl=us"
                ),
                "https://maps.google.com/?cid=5180951040094558101&hl=en&gl=us",
            ],
        )
        self.assertTrue(entry.matched)
        self.assertIsNotNone(entry.place)
        assert entry.place is not None
        self.assertEqual(entry.place.display_name, "Sister Midnight")

    def test_fetch_place_page_enrichment_skips_search_result_with_junk_address(self) -> None:
        place = RawPlace(
            name="Global Village",
            address="Dubai",
            maps_url="https://maps.google.com/?cid=123456789",
            lat=25.0715,
            lng=55.3086,
        )
        called_urls: list[str] = []

        def fake_scrape_place(url: str, **_: object) -> SimpleNamespace:
            called_urls.append(url)
            if "/maps/search/" in url:
                return SimpleNamespace(
                    source_url=url,
                    resolved_url="https://www.google.com/maps/search/?api=1&query=Global+Village",
                    name="Global Village",
                    category="Theme park",
                    rating=4.6,
                    review_count=100,
                    address=(
                        "Imagery ©2026 , Map data ©2026 "
                        "United StatesTermsPrivacySend Product Feedback"
                    ),
                    located_in=None,
                    status=None,
                    website=None,
                    phone=None,
                    plus_code=None,
                    description=None,
                    lat=25.0715,
                    lng=55.3086,
                    limited_view=False,
                )
            return SimpleNamespace(
                source_url=url,
                resolved_url="https://www.google.com/maps/place/Global+Village/@25.0715,55.3086,17z",
                name="Global Village",
                category="Theme park",
                rating=4.6,
                review_count=100,
                address="38C5+F57 - Wadi Al Safa 4 - Dubai - United Arab Emirates",
                located_in=None,
                status=None,
                website="https://www.globalvillage.ae/",
                phone=None,
                plus_code="38C5+F57 Dubai",
                description=None,
                lat=25.0715,
                lng=55.3086,
                limited_view=False,
            )

        with (
            patch.object(build_data, "scrape_place", side_effect=fake_scrape_place),
            patch.object(
                build_data,
                "build_scraper_sessions",
                return_value=(SimpleNamespace(), None, None),
            ),
            patch.object(build_data, "record_scraper_session_use"),
            patch.object(build_data, "release_scraper_session_lock"),
        ):
            entry = build_data.fetch_place_page_enrichment(place)

        self.assertEqual(
            called_urls,
            [
                "https://www.google.com/maps/search/?api=1&query=Global+Village%2C+Dubai&hl=en&gl=us",
                "https://maps.google.com/?cid=123456789&hl=en&gl=us",
            ],
        )
        self.assertTrue(entry.matched)
        assert entry.place is not None
        self.assertEqual(
            entry.place.formatted_address,
            "38C5+F57 - Wadi Al Safa 4 - Dubai - United Arab Emirates",
        )

    def test_normalize_place_page_enrichment_prefers_stable_search_source_url(self) -> None:
        enrichment = build_data.normalize_place_page_enrichment(
            SimpleNamespace(
                source_url=(
                    "https://www.google.com/maps/search/?api=1&query="
                    "Sister+Midnight%2C+4+Rue+Viollet-le-Duc%2C+75009+Paris%2C+France"
                ),
                resolved_url=(
                    "https://www.google.com/maps/place/Sister+Midnight/"
                    "@48.8814703,2.340862,17z/data=!3m1!4b1"
                ),
                name="Sister Midnight",
                category="Cocktail bar",
                rating=4.7,
                review_count=288,
                address="4 Rue Viollet-le-Duc, 75009 Paris, France",
                website="https://sistermidnightparis.com/",
                phone=None,
                plus_code=None,
                description="Volatile review text",
                limited_view=False,
                status=None,
            )
        )

        self.assertEqual(
            enrichment.google_maps_uri,
            (
                "https://www.google.com/maps/search/?api=1&query="
                "Sister+Midnight%2C+4+Rue+Viollet-le-Duc%2C+75009+Paris%2C+France"
            ),
        )
        self.assertIsNone(enrichment.description)

    def test_normalize_place_page_enrichment_carries_photo_urls(self) -> None:
        enrichment = build_data.normalize_place_page_enrichment(
            SimpleNamespace(
                source_url="https://www.google.com/maps/place/Open+Kitchen",
                resolved_url="https://www.google.com/maps/place/Open+Kitchen",
                name="Open Kitchen",
                category="Restaurant",
                rating=4.7,
                review_count=120,
                address="1 Example St, Tokyo",
                website="https://openkitchen.example/",
                phone="+81 3-1111-2222",
                plus_code=None,
                description=None,
                main_photo_url="https://lh3.googleusercontent.com/p/main-example=s680-w680-h510",
                photo_url="https://lh3.googleusercontent.com/p/example=s680-w680-h510",
                limited_view=False,
                status=None,
            )
        )

        self.assertEqual(
            enrichment.main_photo_url,
            "https://lh3.googleusercontent.com/p/main-example=s680-w680-h510",
        )
        self.assertEqual(
            enrichment.photo_url,
            "https://lh3.googleusercontent.com/p/example=s680-w680-h510",
        )

    def test_normalize_place_page_enrichment_drops_suspicious_photo_urls(self) -> None:
        enrichment = build_data.normalize_place_page_enrichment(
            SimpleNamespace(
                source_url="https://www.google.com/maps/place/Open+Kitchen",
                resolved_url="https://www.google.com/maps/place/Open+Kitchen",
                name="Open Kitchen",
                category="Restaurant",
                address="1 Example St, Tokyo",
                main_photo_url=(
                    "https://maps.google.com/maps/api/staticmap?center=35.0%2C139.0"
                    "&zoom=17&size=900x900"
                ),
                photo_url="https://lh3.googleusercontent.com/a-/ALV-UjW_avatar=w36-h36-p-rp-mo-br100",
                limited_view=False,
            )
        )

        self.assertIsNone(enrichment.main_photo_url)
        self.assertIsNone(enrichment.photo_url)

    def test_normalize_place_page_enrichment_preserves_google_place_id_and_address_parts(self) -> None:
        enrichment = build_data.normalize_place_page_enrichment(
            SimpleNamespace(
                source_url="https://www.google.com/maps/place/Elephant+Mountain",
                resolved_url="https://www.google.com/maps/place/Elephant+Mountain",
                google_place_id="ChIJ8T36HxCLGGARvpARPDyaKLA",
                name="象山",
                category="Trailhead",
                address="2HGG+M6",
                address_parts=[
                    "Trailhead",
                    "Xinyi District, Taipei City",
                    "Xinyi District, Taipei City",
                    "Xinyi District",
                    "110",
                    "Taipei City",
                    "TW",
                    ["Taiwan"],
                ],
                limited_view=False,
            )
        )

        self.assertEqual(enrichment.google_place_id, "ChIJ8T36HxCLGGARvpARPDyaKLA")
        self.assertEqual(
            enrichment.address_parts,
            [
                "Trailhead",
                "Xinyi District, Taipei City",
                "Xinyi District, Taipei City",
                "Xinyi District",
                "110",
                "Taipei City",
                "TW",
                ["Taiwan"],
            ],
        )

    def test_sync_guide_place_photos_downloads_local_copy(self) -> None:
        guide = Guide(
            slug="tokyo-japan",
            title="Tokyo, Japan",
            country_name="Japan",
            city_name="Tokyo",
            generated_at="2026-04-20T00:00:00+00:00",
            place_count=1,
            places=[
                NormalizedPlace(
                    id="cid:123",
                    name="Open Kitchen",
                    maps_url="https://maps.google.com/?cid=123",
                    status="active",
                )
            ],
        )
        cache_entry = EnrichmentCacheEntry(
            fetched_at="2026-04-20T00:00:00+00:00",
            query="Open Kitchen",
            matched=True,
            place=EnrichmentPlace(
                display_name="Open Kitchen",
                formatted_address="1 Example St, Tokyo",
                main_photo_url="https://lh3.googleusercontent.com/p/example=s680-w680-h510",
            ),
        )

        image_buffer = BytesIO()
        Image.new("RGB", (1600, 900), color=(200, 120, 80)).save(image_buffer, format="PNG")
        image_bytes = image_buffer.getvalue()

        class FakeHeaders:
            def get_content_type(self) -> str:
                return "image/png"

        class FakeResponse:
            headers = FakeHeaders()

            def read(self) -> bytes:
                return image_bytes

            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, exc_type, exc, tb) -> bool:
                return False

        with TemporaryDirectory() as tmpdir:
            photo_dir = Path(tmpdir)
            photo_url = cache_entry.place.main_photo_url
            assert photo_url is not None
            photo_hash = hashlib.sha256(photo_url.encode("utf-8")).hexdigest()[:12]
            expected_extension = ".webp" if build_data.image_supports_webp() else ".jpg"
            expected_filename = f"cid-123-{photo_hash}{expected_extension}"
            expected_path = photo_dir / expected_filename
            with (
                patch.object(build_data, "PLACE_PHOTOS_DIR", photo_dir),
                patch.object(build_data, "urlopen", return_value=FakeResponse()),
            ):
                build_data.sync_guide_place_photos(
                    guide,
                    enrichment_cache={"cid:123": cache_entry},
                )

            self.assertEqual(
                guide.places[0].main_photo_path,
                f"/place-photos/{expected_filename}",
            )
            self.assertTrue(expected_path.exists())
            with Image.open(expected_path) as generated_image:
                self.assertEqual(
                    generated_image.size,
                    (build_data.PHOTO_CARD_WIDTH, build_data.PHOTO_CARD_HEIGHT),
                )

    def test_sync_place_photo_reuses_existing_hashed_file(self) -> None:
        photo_url = "https://lh3.googleusercontent.com/p/example=s680-w680-h510"
        photo_hash = hashlib.sha256(photo_url.encode("utf-8")).hexdigest()[:12]

        with TemporaryDirectory() as tmpdir:
            photo_dir = Path(tmpdir)
            existing_path = photo_dir / f"cid-123-{photo_hash}.jpg"
            existing_path.parent.mkdir(parents=True, exist_ok=True)
            existing_path.write_bytes(b"existing-image")

            with (
                patch.object(build_data, "PLACE_PHOTOS_DIR", photo_dir),
                patch.object(build_data, "urlopen", side_effect=AssertionError("should not download")),
            ):
                result = build_data.sync_place_photo(
                    "tokyo-japan",
                    "cid:123",
                    photo_url=photo_url,
                )

        self.assertEqual(
            result,
            f"/place-photos/cid-123-{photo_hash}.jpg",
        )

    def test_sync_place_photo_migrates_legacy_guide_scoped_file_to_flat_storage(self) -> None:
        photo_url = "https://lh3.googleusercontent.com/p/example=s680-w680-h510"
        photo_hash = hashlib.sha256(photo_url.encode("utf-8")).hexdigest()[:12]

        with TemporaryDirectory() as tmpdir:
            photo_dir = Path(tmpdir)
            legacy_path = photo_dir / "tokyo-japan" / f"cid-123-{photo_hash}.jpg"
            legacy_path.parent.mkdir(parents=True, exist_ok=True)
            legacy_path.write_bytes(b"existing-image")

            with (
                patch.object(build_data, "PLACE_PHOTOS_DIR", photo_dir),
                patch.object(build_data, "urlopen", side_effect=AssertionError("should not download")),
            ):
                result = build_data.sync_place_photo(
                    "tokyo-japan",
                    "cid:123",
                    photo_url=photo_url,
                )

            canonical_path = photo_dir / f"cid-123-{photo_hash}.jpg"
            self.assertEqual(result, f"/place-photos/cid-123-{photo_hash}.jpg")
            self.assertTrue(canonical_path.exists())
            self.assertFalse(legacy_path.exists())

    def test_populate_place_photos_for_guides_parallelizes_refresh_jobs(self) -> None:
        guide = Guide(
            slug="tokyo-japan",
            title="Tokyo, Japan",
            country_name="Japan",
            city_name="Tokyo",
            generated_at="2026-04-20T00:00:00+00:00",
            place_count=2,
            places=[
                NormalizedPlace(
                    id="cid:111",
                    name="First Place",
                    maps_url="https://maps.google.com/?cid=111",
                    status="active",
                ),
                NormalizedPlace(
                    id="cid:222",
                    name="Second Place",
                    maps_url="https://maps.google.com/?cid=222",
                    status="active",
                ),
            ],
        )
        enrichment_caches = {
            "tokyo-japan": {
                "cid:111": EnrichmentCacheEntry(
                    fetched_at="2026-04-20T00:00:00+00:00",
                    query="First Place",
                    matched=True,
                    place=EnrichmentPlace(main_photo_url="https://example.com/first.jpg"),
                ),
                "cid:222": EnrichmentCacheEntry(
                    fetched_at="2026-04-20T00:00:00+00:00",
                    query="Second Place",
                    matched=True,
                    place=EnrichmentPlace(main_photo_url="https://example.com/second.jpg"),
                ),
            }
        }
        executor_holder: dict[str, object] = {}
        calls: list[tuple[str, str, str | None, float]] = []

        class FakeFuture:
            def __init__(self, value: str | None):
                self._value = value

            def result(self) -> str | None:
                return self._value

        class FakeExecutor:
            def __init__(self, max_workers: int):
                self.max_workers = max_workers
                executor_holder["executor"] = self

            def __enter__(self) -> "FakeExecutor":
                return self

            def __exit__(self, exc_type, exc, tb) -> bool:
                return False

            def submit(self, fn, slug, place_id, *, photo_url, startup_jitter_seconds):
                value = fn(
                    slug,
                    place_id,
                    photo_url=photo_url,
                    startup_jitter_seconds=startup_jitter_seconds,
                )
                return FakeFuture(value)

        def fake_sync_place_photo(
            slug: str,
            place_id: str,
            *,
            photo_url: str | None,
            startup_jitter_seconds: float = 0,
        ) -> str:
            calls.append((slug, place_id, photo_url, startup_jitter_seconds))
            return f"/place-photos/{place_id}.webp"

        with (
            patch("builtins.print") as print_mock,
            patch.object(build_data, "ThreadPoolExecutor", FakeExecutor),
            patch.object(build_data, "as_completed", side_effect=lambda futures: list(futures)),
            patch.object(build_data, "sync_place_photo", side_effect=fake_sync_place_photo),
        ):
            build_data.populate_place_photos_for_guides(
                [guide],
                enrichment_caches=enrichment_caches,
                refresh_photos=True,
                photo_workers=4,
                startup_jitter_seconds=8,
            )

        executor = executor_holder["executor"]
        assert isinstance(executor, FakeExecutor)
        self.assertEqual(executor.max_workers, 2)
        self.assertEqual(
            calls,
            [
                ("tokyo-japan", "cid:111", "https://example.com/first.jpg", 8),
                ("tokyo-japan", "cid:222", "https://example.com/second.jpg", 8),
            ],
        )
        self.assertEqual(guide.places[0].main_photo_path, "/place-photos/cid:111.webp")
        self.assertEqual(guide.places[1].main_photo_path, "/place-photos/cid:222.webp")
        self.assertEqual(
            [args[0] for args, _kwargs in print_mock.call_args_list],
            [
                "Downloading 2 place photos with 2 workers (0 existing, 0 without photo URLs)",
                "[photos 1/2] downloaded: tokyo-japan / First Place",
                "[photos 2/2] downloaded: tokyo-japan / Second Place",
            ],
        )

    def test_populate_place_photos_for_guides_uses_existing_local_files_without_refresh(self) -> None:
        guide = Guide(
            slug="tokyo-japan",
            title="Tokyo, Japan",
            country_name="Japan",
            city_name="Tokyo",
            generated_at="2026-04-20T00:00:00+00:00",
            place_count=1,
            places=[
                NormalizedPlace(
                    id="cid:123",
                    name="Open Kitchen",
                    maps_url="https://maps.google.com/?cid=123",
                    status="active",
                )
            ],
        )
        photo_url = "https://lh3.googleusercontent.com/p/example=s680-w680-h510"
        photo_hash = hashlib.sha256(photo_url.encode("utf-8")).hexdigest()[:12]

        with TemporaryDirectory() as tmpdir:
            photo_dir = Path(tmpdir)
            existing_path = photo_dir / f"cid-123-{photo_hash}.jpg"
            existing_path.parent.mkdir(parents=True, exist_ok=True)
            existing_path.write_bytes(b"existing-image")

            with (
                patch.object(build_data, "PLACE_PHOTOS_DIR", photo_dir),
                patch.object(build_data, "sync_place_photo", side_effect=AssertionError("should not refresh")),
            ):
                build_data.populate_place_photos_for_guides(
                    [guide],
                    enrichment_caches={
                        "tokyo-japan": {
                            "cid:123": EnrichmentCacheEntry(
                                fetched_at="2026-04-20T00:00:00+00:00",
                                query="Open Kitchen",
                                matched=True,
                                place=EnrichmentPlace(main_photo_url=photo_url),
                            )
                        }
                    },
                    refresh_photos=False,
                    photo_workers=4,
                    startup_jitter_seconds=8,
                )

        self.assertEqual(
            guide.places[0].main_photo_path,
            f"/place-photos/cid-123-{photo_hash}.jpg",
        )

    def test_populate_place_photos_for_guides_keeps_existing_photo_when_url_changes_and_refresh_is_disabled(
        self,
    ) -> None:
        guide = Guide(
            slug="tokyo-japan",
            title="Tokyo, Japan",
            country_name="Japan",
            city_name="Tokyo",
            generated_at="2026-04-20T00:00:00+00:00",
            place_count=1,
            places=[
                NormalizedPlace(
                    id="cid:123",
                    name="Open Kitchen",
                    maps_url="https://maps.google.com/?cid=123",
                    status="active",
                )
            ],
        )
        old_photo_url = "https://lh3.googleusercontent.com/p/old-token=s680-w680-h510"
        new_photo_url = "https://lh3.googleusercontent.com/p/new-token=s680-w680-h510"
        old_photo_hash = hashlib.sha256(old_photo_url.encode("utf-8")).hexdigest()[:12]

        with TemporaryDirectory() as tmpdir:
            photo_dir = Path(tmpdir)
            existing_path = photo_dir / f"cid-123-{old_photo_hash}.jpg"
            existing_path.write_bytes(b"existing-image")

            with (
                patch.object(build_data, "PLACE_PHOTOS_DIR", photo_dir),
                patch.object(build_data, "sync_place_photo", side_effect=AssertionError("should not refresh")),
            ):
                build_data.populate_place_photos_for_guides(
                    [guide],
                    enrichment_caches={
                        "tokyo-japan": {
                            "cid:123": EnrichmentCacheEntry(
                                fetched_at="2026-04-20T00:00:00+00:00",
                                query="Open Kitchen",
                                matched=True,
                                place=EnrichmentPlace(main_photo_url=new_photo_url),
                            )
                        }
                    },
                    refresh_photos=False,
                    photo_workers=4,
                    startup_jitter_seconds=8,
                )

        self.assertEqual(
            guide.places[0].main_photo_path,
            f"/place-photos/cid-123-{old_photo_hash}.jpg",
        )

    def test_populate_place_photos_for_guides_warns_when_reusing_photo_because_enrichment_lost_photo_url(
        self,
    ) -> None:
        guide = Guide(
            slug="tokyo-japan",
            title="Tokyo, Japan",
            country_name="Japan",
            city_name="Tokyo",
            generated_at="2026-04-20T00:00:00+00:00",
            place_count=1,
            places=[
                NormalizedPlace(
                    id="cid:123",
                    name="Open Kitchen",
                    maps_url="https://maps.google.com/?cid=123",
                    status="active",
                )
            ],
        )
        old_photo_url = "https://lh3.googleusercontent.com/p/old-token=s680-w680-h510"
        old_photo_hash = hashlib.sha256(old_photo_url.encode("utf-8")).hexdigest()[:12]

        with TemporaryDirectory() as tmpdir:
            photo_dir = Path(tmpdir)
            existing_path = photo_dir / f"cid-123-{old_photo_hash}.jpg"
            existing_path.write_bytes(b"existing-image")

            stdout = StringIO()
            with (
                patch.object(build_data, "PLACE_PHOTOS_DIR", photo_dir),
                patch.object(build_data, "sync_place_photo", side_effect=AssertionError("should not refresh")),
                redirect_stdout(stdout),
            ):
                build_data.populate_place_photos_for_guides(
                    [guide],
                    enrichment_caches={
                        "tokyo-japan": {
                            "cid:123": EnrichmentCacheEntry(
                                fetched_at="2026-04-20T00:00:00+00:00",
                                query="Open Kitchen",
                                matched=True,
                                place=EnrichmentPlace(),
                            )
                        }
                    },
                    refresh_photos=False,
                    photo_workers=4,
                    startup_jitter_seconds=8,
                )

        self.assertEqual(
            guide.places[0].main_photo_path,
            f"/place-photos/cid-123-{old_photo_hash}.jpg",
        )
        self.assertIn("Photo extraction may be failing on this runner.", stdout.getvalue())

    def test_optimize_place_photo_asset_resizes_to_card_ratio(self) -> None:
        source = BytesIO()
        Image.new("RGB", (1400, 1000), color=(10, 50, 200)).save(source, format="PNG")

        optimized_content, extension = build_data.optimize_place_photo_asset(
            source.getvalue(),
            content_type="image/png",
        )

        self.assertIsNotNone(optimized_content)
        self.assertIn(extension, {".webp", ".jpg"})
        assert optimized_content is not None
        with Image.open(BytesIO(optimized_content)) as optimized_image:
            self.assertEqual(
                optimized_image.size,
                (build_data.PHOTO_CARD_WIDTH, build_data.PHOTO_CARD_HEIGHT),
            )

    def test_place_page_has_meaningful_enrichment_rejects_address_only_false_positive(self) -> None:
        self.assertFalse(
            build_data.place_page_has_meaningful_enrichment(
                SimpleNamespace(limited_view=False),
                EnrichmentPlace(
                    display_name=None,
                    formatted_address="バー · 26-28 Cotham Rd",
                ),
            )
        )

    def test_place_page_has_meaningful_enrichment_accepts_name_address_and_reputation(self) -> None:
        self.assertTrue(
            build_data.place_page_has_meaningful_enrichment(
                SimpleNamespace(limited_view=False),
                EnrichmentPlace(
                    display_name="Bianchetto",
                    formatted_address="26-28 Cotham Rd, Kew VIC 3101, Australia",
                    rating=5.0,
                    user_rating_count=8,
                ),
            )
        )

    def test_place_page_has_meaningful_enrichment_rejects_ui_label_false_positive(self) -> None:
        self.assertFalse(
            build_data.place_page_has_meaningful_enrichment(
                SimpleNamespace(limited_view=False),
                EnrichmentPlace(
                    display_name=None,
                    formatted_address="バー · 26-28 Cotham Rd",
                    primary_type_display_name="バー",
                ),
            )
        )

    def test_should_fallback_to_places_api_for_sparse_search_result(self) -> None:
        entry = EnrichmentCacheEntry(
            fetched_at="2026-04-20T00:00:00+00:00",
            query="Bianchetto, 26-28 Cotham Rd, Kew VIC 3101, Australia",
            source="google_maps_page",
            matched=True,
            score=45,
            place=EnrichmentPlace(
                display_name="Bianchetto",
                formatted_address="バー · 26-28 Cotham Rd",
                google_maps_uri=(
                    "https://www.google.com/maps/search/?api=1&query="
                    "Bianchetto%2C+26-28+Cotham+Rd%2C+Kew+VIC+3101%2C+Australia"
                ),
                primary_type_display_name="バー",
            ),
        )

        self.assertTrue(build_data.should_fallback_to_places_api(entry))

    def test_should_not_fallback_to_places_api_for_rich_search_result(self) -> None:
        entry = EnrichmentCacheEntry(
            fetched_at="2026-04-20T00:00:00+00:00",
            query="Sister Midnight, 4 Rue Viollet-le-Duc, 75009 Paris, France",
            source="google_maps_page",
            matched=True,
            score=45,
            place=EnrichmentPlace(
                display_name="Sister Midnight",
                formatted_address="4 Rue Viollet-le-Duc, 75009 Paris, France",
                google_maps_uri=(
                    "https://www.google.com/maps/search/?api=1&query="
                    "Sister+Midnight%2C+4+Rue+Viollet-le-Duc%2C+75009+Paris%2C+France"
                ),
                rating=4.7,
                user_rating_count=288,
                website="https://sistermidnightparis.com/",
                primary_type_display_name="Cocktail bar",
            ),
        )

        self.assertFalse(build_data.should_fallback_to_places_api(entry))

    def test_extract_hashtags_normalizes_accented_and_malformed_location_tags(self) -> None:
        tags = build_data.extract_hashtags("Great spots #genève #gen-ve #park")
        self.assertEqual(tags, ["geneve", "park"])

    def test_stable_place_id_fallback_uses_plain_slugify_output(self) -> None:
        place = RawPlace(
            name="Gen-ve Cafe",
            address="1 Example St, Geneva, Switzerland",
            maps_url="https://maps.google.com/?q=Gen-ve+Cafe",
            lat=46.2044,
            lng=6.1432,
        )

        self.assertEqual(
            build_data.stable_place_id(place),
            "slug:gen-ve-cafe-46-2044-6-1432",
        )

    def test_stable_place_id_fallback_does_not_fold_accents(self) -> None:
        place = RawPlace(
            name="Genève Cafe",
            address="1 Example St, Geneva, Switzerland",
            maps_url="https://maps.google.com/?q=Gen%C3%A8ve+Cafe",
            lat=46.2044,
            lng=6.1432,
        )

        self.assertEqual(
            build_data.stable_place_id(place),
            "slug:gen-ve-cafe-46-2044-6-1432",
        )

    def test_normalize_guide_expands_location_aliases_in_list_tags(self) -> None:
        raw = RawSavedList(
            title="Genève, Switzerland",
            description="Walkable guide #genève #park",
            places=[
                RawPlace(
                    name="Parc de La Perle du Lac",
                    address="Rue de Lausanne 120B, 1202 Genève, Switzerland",
                    maps_url="https://maps.google.com/?cid=1",
                    cid="111",
                )
            ],
        )

        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            list_overrides_dir = tmpdir_path / "lists"
            place_overrides_dir = tmpdir_path / "places"
            list_overrides_dir.mkdir()
            place_overrides_dir.mkdir()

            (list_overrides_dir / "geneve-switzerland.json").write_text(
                json.dumps(
                    {
                        "city_name": "Genève",
                        "country_name": "Switzerland",
                        "country_code": "CH",
                    }
                ),
                encoding="utf-8",
            )

            with (
                patch.object(build_data, "LIST_OVERRIDES_DIR", list_overrides_dir),
                patch.object(build_data, "PLACE_OVERRIDES_DIR", place_overrides_dir),
            ):
                guide = build_data.normalize_guide("geneve-switzerland", raw, enrichment_cache={})

        self.assertEqual(guide.list_tags, ["geneva", "geneve", "park"])

    def test_normalize_guide_normalizes_accented_override_list_tags(self) -> None:
        raw = RawSavedList(
            title="Genève, Switzerland",
            places=[
                RawPlace(
                    name="Parc de La Perle du Lac",
                    address="Rue de Lausanne 120B, 1202 Genève, Switzerland",
                    maps_url="https://maps.google.com/?cid=1",
                    cid="111",
                )
            ],
        )

        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            list_overrides_dir = tmpdir_path / "lists"
            place_overrides_dir = tmpdir_path / "places"
            list_overrides_dir.mkdir()
            place_overrides_dir.mkdir()

            (list_overrides_dir / "geneve-switzerland.json").write_text(
                json.dumps({"list_tags": ["Genève", "Park"]}),
                encoding="utf-8",
            )

            with (
                patch.object(build_data, "LIST_OVERRIDES_DIR", list_overrides_dir),
                patch.object(build_data, "PLACE_OVERRIDES_DIR", place_overrides_dir),
            ):
                guide = build_data.normalize_guide("geneve-switzerland", raw, enrichment_cache={})

        self.assertEqual(guide.list_tags, ["geneva", "geneve", "park"])

    def test_normalize_guide_skips_placeholder_override_list_tags(self) -> None:
        raw = RawSavedList(
            title="Genève, Switzerland",
            places=[
                RawPlace(
                    name="Parc de La Perle du Lac",
                    address="Rue de Lausanne 120B, 1202 Genève, Switzerland",
                    maps_url="https://maps.google.com/?cid=1",
                    cid="111",
                )
            ],
        )

        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            list_overrides_dir = tmpdir_path / "lists"
            place_overrides_dir = tmpdir_path / "places"
            list_overrides_dir.mkdir()
            place_overrides_dir.mkdir()

            (list_overrides_dir / "geneve-switzerland.json").write_text(
                json.dumps({"list_tags": ["🌯", "東京", "Park"]}),
                encoding="utf-8",
            )

            with (
                patch.object(build_data, "LIST_OVERRIDES_DIR", list_overrides_dir),
                patch.object(build_data, "PLACE_OVERRIDES_DIR", place_overrides_dir),
            ):
                guide = build_data.normalize_guide("geneve-switzerland", raw, enrichment_cache={})

        self.assertEqual(guide.list_tags, ["park"])

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
        self.assertEqual(first_place.marker_icon, "bakery")
        self.assertEqual(first_place.note, "Manual note")
        self.assertEqual(
            first_place.maps_url,
            "https://www.google.com/maps/search/?api=1&query=Coffee+House%2C+1+Shibuya%2C+Tokyo%2C+Japan",
        )
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

    def test_normalize_guide_marks_enriched_neighborhood_provenance_when_raw_address_missing(self) -> None:
        raw = RawSavedList(
            title="Taipei, Taiwan",
            places=[
                RawPlace(
                    name="象山",
                    maps_url="https://maps.google.com/?cid=111",
                    cid="111",
                ),
            ],
        )
        place_id = build_data.stable_place_id(raw.places[0])
        enrichment_cache = {
            place_id: EnrichmentCacheEntry(
                fetched_at="2026-04-26T00:00:00+00:00",
                query="象山, Taipei, Taiwan",
                matched=True,
                source="google_maps_page",
                place=EnrichmentPlace(
                    formatted_address="2HGG+M6",
                    primary_type="trailhead",
                    primary_type_display_name="Trailhead",
                    address_parts=[
                        "Trailhead",
                        "Xinyi District, Taipei City",
                        "Xinyi District, Taipei City",
                        "Xinyi District",
                        "110",
                        "Taipei City",
                        "TW",
                        ["Taiwan"],
                    ],
                ),
            ),
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

        self.assertEqual(guide.places[0].neighborhood, "Xinyi District")
        self.assertIsNotNone(guide.places[0].provenance.neighborhood)
        assert guide.places[0].provenance.neighborhood is not None
        self.assertEqual(guide.places[0].provenance.neighborhood.source, "google_maps_page")

    def test_normalize_guide_excludes_permanently_closed_places_from_ui_counts(self) -> None:
        raw = RawSavedList(
            title="Tokyo, Japan",
            places=[
                RawPlace(
                    name="Active Coffee",
                    address="1 Shibuya, Tokyo, Japan",
                    maps_url="https://maps.google.com/?cid=1",
                    cid="111",
                    lat=35.66,
                    lng=139.7,
                ),
                RawPlace(
                    name="Closed Cafe",
                    address="2 Shibuya, Tokyo, Japan",
                    is_favorite=True,
                    maps_url="https://maps.google.com/?cid=2",
                    cid="222",
                    lat=40.0,
                    lng=140.0,
                ),
                RawPlace(
                    name="Temporary Tea",
                    address="3 Shibuya, Tokyo, Japan",
                    is_favorite=True,
                    maps_url="https://maps.google.com/?cid=3",
                    cid="333",
                    lat=35.68,
                    lng=139.72,
                ),
            ],
        )
        active_place_id = build_data.stable_place_id(raw.places[0])
        closed_place_id = build_data.stable_place_id(raw.places[1])
        temporary_place_id = build_data.stable_place_id(raw.places[2])
        enrichment_cache = {
            active_place_id: EnrichmentCacheEntry(
                fetched_at="2026-04-01T00:00:00+00:00",
                query="Active Coffee",
                matched=True,
                place=EnrichmentPlace(business_status="OPERATIONAL"),
            ),
            closed_place_id: EnrichmentCacheEntry(
                fetched_at="2026-04-01T00:00:00+00:00",
                query="Closed Cafe",
                matched=True,
                place=EnrichmentPlace(business_status="CLOSED_PERMANENTLY"),
            ),
            temporary_place_id: EnrichmentCacheEntry(
                fetched_at="2026-04-01T00:00:00+00:00",
                query="Temporary Tea",
                matched=True,
                place=EnrichmentPlace(business_status="CLOSED_TEMPORARILY"),
            ),
        }

        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            list_overrides_dir = tmpdir_path / "lists"
            place_overrides_dir = tmpdir_path / "places"
            list_overrides_dir.mkdir()
            place_overrides_dir.mkdir()
            (list_overrides_dir / "tokyo-japan.json").write_text(
                json.dumps({"featured_place_ids": [closed_place_id, temporary_place_id]}),
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

        self.assertEqual(guide.place_count, 2)
        self.assertEqual(guide.featured_place_ids, [temporary_place_id])
        self.assertAlmostEqual(guide.center_lat or 0, 35.67, places=2)
        self.assertAlmostEqual(guide.center_lng or 0, 139.71, places=2)
        self.assertEqual(
            {place.id: place.status for place in guide.places},
            {
                active_place_id: "active",
                closed_place_id: "closed-permanently",
                temporary_place_id: "temporarily-closed",
            },
        )

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

    def test_derive_marker_icon_prefers_specific_type_matches(self) -> None:
        test_cases = [
            (
                "coffee-shop",
                EnrichmentPlace(primary_type="restaurant", types=["coffee_shop", "restaurant"]),
                "cafe",
            ),
            (
                "art-gallery",
                EnrichmentPlace(types=["art_gallery"]),
                "museum",
            ),
            (
                None,
                EnrichmentPlace(types=["historical_landmark"]),
                "attraction",
            ),
            (
                None,
                EnrichmentPlace(types=["spa"]),
                "spa",
            ),
            (
                "night market",
                EnrichmentPlace(),
                "shopping",
            ),
        ]

        for category, enrichment, expected_icon in test_cases:
            with self.subTest(category=category, expected_icon=expected_icon):
                self.assertEqual(
                    build_data.derive_marker_icon(
                        RawPlace(name="Plain Place", maps_url="https://maps.google.com/?cid=1"),
                        enrichment=enrichment,
                        category=category,
                        note=None,
                        why_recommended=None,
                    ),
                    expected_icon,
                )

    def test_derive_marker_icon_falls_back_to_default_without_type_signal(self) -> None:
        self.assertEqual(
            build_data.derive_marker_icon(
                RawPlace(name="Plain Place", maps_url="https://maps.google.com/?cid=1"),
                enrichment=EnrichmentPlace(),
                category=None,
                note=None,
                why_recommended=None,
            ),
            "default",
        )

    def test_derive_enrichment_type_tags_expands_parent_category_tags(self) -> None:
        self.assertEqual(
            build_data.derive_enrichment_type_tags(
                EnrichmentPlace(primary_type="peruvian_restaurant", types=["peruvian_restaurant"])
            ),
            ["restaurant", "peruvian-restaurant"],
        )
        self.assertEqual(
            build_data.derive_enrichment_type_tags(
                EnrichmentPlace(primary_type="coffee_shop", types=["coffee_shop"])
            ),
            ["cafe", "coffee-shop"],
        )
        self.assertEqual(
            build_data.derive_enrichment_type_tags(
                EnrichmentPlace(primary_type="art_gallery", types=["art_gallery"])
            ),
            ["museum", "art-gallery"],
        )
        self.assertEqual(
            build_data.derive_enrichment_type_tags(
                EnrichmentPlace(primary_type="udon_noodle_restaurant", types=["udon_noodle_restaurant"])
            ),
            ["restaurant", "japanese-restaurant", "udon-noodle-restaurant"],
        )
        self.assertEqual(
            build_data.derive_enrichment_type_tags(
                EnrichmentPlace(primary_type="steak_house", types=["steak_house"])
            ),
            ["restaurant", "western-restaurant", "steak-house"],
        )

    def test_normalize_place_page_enrichment_infers_localized_category_types(self) -> None:
        steakhouse = build_data.normalize_place_page_enrichment(
            SimpleNamespace(
                source_url="https://www.google.com/maps/place/Test",
                resolved_url="https://www.google.com/maps/place/Test",
                name="Test Steakhouse",
                category="和風ステーキハウス",
                rating=4.6,
                review_count=128,
                address="Kyoto",
                limited_view=False,
            )
        )
        self.assertEqual(steakhouse.primary_type, "steak_house")
        self.assertEqual(steakhouse.primary_type_display_name, "Steak house")
        self.assertEqual(steakhouse.primary_type_display_name_localized, "和風ステーキハウス")
        self.assertEqual(steakhouse.types, ["restaurant", "western_restaurant", "steak_house"])

        museum = build_data.normalize_place_page_enrichment(
            SimpleNamespace(
                source_url="https://www.google.com/maps/place/Test",
                resolved_url="https://www.google.com/maps/place/Test",
                name="Museum",
                category="博物館",
                rating=4.8,
                review_count=240,
                address="Taoyuan",
                limited_view=False,
            )
        )
        self.assertEqual(museum.primary_type, "museum")
        self.assertEqual(museum.primary_type_display_name, "Museum")
        self.assertEqual(museum.primary_type_display_name_localized, "博物館")
        self.assertEqual(museum.types, ["museum"])

    def test_normalize_place_page_enrichment_drops_suspicious_category_labels(self) -> None:
        place = build_data.normalize_place_page_enrichment(
            SimpleNamespace(
                source_url="https://www.google.com/maps/place/Test",
                resolved_url="https://www.google.com/maps/place/Test",
                name="Test Place",
                category="Floor 1",
                rating=4.2,
                review_count=17,
                address="Osaka",
                limited_view=False,
            )
        )

        self.assertIsNone(place.primary_type)
        self.assertIsNone(place.primary_type_display_name)
        self.assertIsNone(place.primary_type_display_name_localized)
        self.assertEqual(place.types, [])

    def test_normalize_place_page_enrichment_sanitizes_suspicious_addresses(self) -> None:
        chrome = build_data.normalize_place_page_enrichment(
            SimpleNamespace(
                source_url="https://www.google.com/maps/place/Dubai+Frame",
                resolved_url="https://www.google.com/maps/place/Dubai+Frame",
                name="Dubai Frame",
                category="Tourist attraction",
                rating=4.6,
                review_count=100,
                address=(
                    "Imagery ©2026 , Map data ©2026 "
                    "United StatesTermsPrivacySend Product Feedback"
                ),
                limited_view=False,
            )
        )
        token = build_data.normalize_place_page_enrichment(
            SimpleNamespace(
                source_url="https://www.google.com/maps/place/Jumeirah+Beach",
                resolved_url="https://www.google.com/maps/place/Jumeirah+Beach",
                name="Jumeirah Beach",
                category="Beach",
                rating=4.4,
                review_count=20,
                address="/m/0cnyfm6",
                limited_view=False,
            )
        )
        street_view = build_data.normalize_place_page_enrichment(
            SimpleNamespace(
                source_url="https://www.google.com/maps/place/Jumeirah+Beach",
                resolved_url="https://www.google.com/maps/place/Jumeirah+Beach",
                name="Jumeirah Beach",
                category="Beach",
                rating=4.4,
                review_count=20,
                address="Street View & 360°",
                limited_view=False,
            )
        )
        prefixed = build_data.normalize_place_page_enrichment(
            SimpleNamespace(
                source_url="https://www.google.com/maps/place/Global+Village",
                resolved_url="https://www.google.com/maps/place/Global+Village",
                name="Global Village",
                category="Theme park",
                rating=4.5,
                review_count=20,
                address="企業のオフィス ·  · Exit 37 - Sheikh Mohammed Bin Zayed Rd",
                limited_view=False,
            )
        )

        self.assertIsNone(chrome.formatted_address)
        self.assertIsNone(token.formatted_address)
        self.assertIsNone(street_view.formatted_address)
        self.assertEqual(prefixed.formatted_address, "Exit 37 - Sheikh Mohammed Bin Zayed Rd")

    def test_normalize_place_page_enrichment_falls_back_to_compound_plus_code(self) -> None:
        place = build_data.normalize_place_page_enrichment(
            SimpleNamespace(
                source_url="https://www.google.com/maps/place/Global+Village",
                resolved_url="https://www.google.com/maps/place/Global+Village",
                name="Global Village",
                category="Theme park",
                rating=4.5,
                review_count=20,
                address="Street View & 360°",
                plus_code="38C5+F57 - Wadi Al Safa 4 - Dubai - United Arab Emirates",
                limited_view=False,
            )
        )

        self.assertEqual(
            place.formatted_address,
            "38C5+F57 - Wadi Al Safa 4 - Dubai - United Arab Emirates",
        )

    def test_normalize_place_page_enrichment_uses_address_like_description_when_address_missing(self) -> None:
        place = build_data.normalize_place_page_enrichment(
            SimpleNamespace(
                source_url="https://www.google.com/maps/search/?api=1&query=Jumeirah+Beach",
                resolved_url="https://www.google.com/maps/place/Jumeirah+Beach",
                name="Jumeirah Beach",
                category="Beach",
                rating=4.6,
                review_count=None,
                address=None,
                plus_code=None,
                description="Jumeirah Beach - Jumeirah - Jumeira Third - Dubai - United Arab Emirates",
                limited_view=True,
            )
        )

        self.assertEqual(
            place.formatted_address,
            "Jumeirah Beach - Jumeirah - Jumeira Third - Dubai - United Arab Emirates",
        )
        self.assertIsNone(place.description)

    def test_normalize_guide_keeps_english_primary_category_and_stores_localized_variant(self) -> None:
        raw = RawSavedList(
            title="Fukuoka",
            places=[
                RawPlace(
                    name="やまや別邸西中洲",
                    address="Fukuoka",
                    maps_url="https://maps.google.com/?cid=111",
                    cid="111",
                )
            ],
        )
        place_id = build_data.stable_place_id(raw.places[0])
        enrichment_cache = {
            place_id: EnrichmentCacheEntry(
                fetched_at="2026-04-23T00:00:00+00:00",
                query="やまや別邸西中洲, Fukuoka",
                matched=True,
                score=45,
                place=EnrichmentPlace(
                    primary_type="restaurant",
                    primary_type_display_name="もつ鍋料理店",
                ),
            )
        }

        guide = build_data.normalize_guide("fukuoka-japan", raw, enrichment_cache=enrichment_cache)
        place = guide.places[0]

        self.assertEqual(place.primary_category, "Restaurant")
        self.assertEqual(place.primary_category_localized, "もつ鍋料理店")
        self.assertEqual(place.provenance.primary_category.value, "Restaurant")
        self.assertEqual(place.provenance.primary_category_localized.value, "もつ鍋料理店")

    def test_normalize_enrichment_match_expands_parent_restaurant_types(self) -> None:
        place = build_data.normalize_enrichment_match(
            {
                "id": "test-id",
                "name": "places/test-id",
                "displayName": {"text": "Test Udon"},
                "formattedAddress": "Sapporo",
                "googleMapsUri": "https://maps.google.com/?cid=1",
                "primaryType": "udon_noodle_restaurant",
                "primaryTypeDisplayName": {"text": "Udon noodle restaurant"},
                "types": ["udon_noodle_restaurant"],
            }
        )

        self.assertEqual(place.primary_type, "udon_noodle_restaurant")
        self.assertEqual(
            place.types,
            ["restaurant", "japanese_restaurant", "udon_noodle_restaurant"],
        )

    def test_derive_marker_icon_uses_place_name_keyword_fallback_without_enrichment(self) -> None:
        test_cases = [
            ("Bar Rooster", "bar"),
            ("Bear Pond Espresso", "cafe"),
            ("Ameyoko Shopping District", "shopping"),
            ("Pizza Strada", "restaurant"),
        ]

        for place_name, expected_icon in test_cases:
            with self.subTest(place_name=place_name):
                self.assertEqual(
                    build_data.derive_marker_icon(
                        RawPlace(name=place_name, maps_url="https://maps.google.com/?cid=1"),
                        enrichment=EnrichmentPlace(),
                        category=None,
                        note=None,
                        why_recommended=None,
                    ),
                    expected_icon,
                )

    def test_derive_marker_icon_does_not_use_locality_tags_as_type_fallback(self) -> None:
        self.assertEqual(
            build_data.derive_marker_icon(
                RawPlace(
                    name="Plain Place",
                    address="1 Ocean Drive, Miami Beach, Park City",
                    maps_url="https://maps.google.com/?cid=1",
                ),
                enrichment=EnrichmentPlace(),
                category=None,
                note=None,
                why_recommended=None,
            ),
            "default",
        )

    def test_derive_place_tags_prefers_curated_enrichment_tags(self) -> None:
        tags = build_data.derive_place_tags(
            RawPlace(
                name="Coffee Counter",
                address="123 Main St, Williamsburg, Brooklyn, United States",
                maps_url="https://maps.google.com/?cid=1",
            ),
            "New York",
            enrichment=EnrichmentPlace(
                primary_type="coffee_shop",
                primary_type_display_name="Coffee shop",
                types=["coffee_shop", "food", "point_of_interest", "establishment"],
            ),
            category="Coffee shop",
        )

        self.assertIn("cafe", tags)
        self.assertIn("coffee-shop", tags)
        self.assertIn("food", tags)
        self.assertNotIn("point-of-interest", tags)
        self.assertNotIn("establishment", tags)

    def test_derive_place_tags_downweights_broad_localities_when_enrichment_is_specific(self) -> None:
        tags = build_data.derive_place_tags(
            RawPlace(
                name="Neighborhood Cafe",
                address="123 Main St, Williamsburg, Brooklyn, United States",
                maps_url="https://maps.google.com/?cid=1",
            ),
            "New York",
            enrichment=EnrichmentPlace(primary_type="cafe", types=["cafe", "food"]),
            category="Cafe",
        )

        self.assertIn("williamsburg", tags)
        self.assertNotIn("brooklyn", tags)
        self.assertIn("cafe", tags)

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
        self.assertEqual(index["generated_at"], build_data.STABLE_GENERATED_AT_FALLBACK)
        self.assertEqual(index["guides"][0]["slug"], "tokyo-japan")
        self.assertEqual(index["entries"][0]["guide_slug"], "tokyo-japan")
        self.assertEqual(index["entries"][0]["name"], "Quiet Coffee")
        self.assertIn("quiet", index["entries"][0]["vibe_tags"])
        self.assertIn("laptop-friendly", index["entries"][0]["vibe_tags"])
        self.assertIn("tokyo", index["entries"][0]["search_text"])

    def test_normalize_guide_uses_stable_generated_at_from_inputs(self) -> None:
        raw = RawSavedList(
            fetched_at="2026-04-18T00:00:00+00:00",
            title="Tokyo, Japan",
            places=[
                RawPlace(
                    name="Quiet Coffee",
                    address="1 Shibuya, Tokyo, Japan",
                    maps_url="https://maps.google.com/?cid=1",
                    cid="111",
                ),
            ],
        )
        place_id = build_data.stable_place_id(raw.places[0], source_type=raw.configured_source_type)
        guide = build_data.normalize_guide(
            "tokyo-japan",
            raw,
            enrichment_cache={
                place_id: EnrichmentCacheEntry(
                    fetched_at="2026-04-19T00:00:00+00:00",
                    last_verified_at="2026-04-20T00:00:00+00:00",
                    query="Quiet Coffee, 1 Shibuya, Tokyo, Japan",
                ),
            },
        )

        self.assertEqual(guide.generated_at, "2026-04-20T00:00:00+00:00")

    def test_build_search_index_includes_rating_fields_when_present_or_missing(self) -> None:
        guide = Guide(
            slug="tokyo-japan",
            title="Tokyo, Japan",
            country_name="Japan",
            city_name="Tokyo",
            generated_at="2026-04-20T00:00:00+00:00",
            place_count=2,
            places=[
                NormalizedPlace(
                    id="rated",
                    name="Rated Coffee",
                    maps_url="https://maps.example/rated",
                    rating=4.8,
                    user_rating_count=321,
                    status="active",
                ),
                NormalizedPlace(
                    id="unrated",
                    name="Unrated Tea",
                    maps_url="https://maps.example/unrated",
                    rating=None,
                    user_rating_count=None,
                    status="active",
                ),
            ],
        )

        index = build_data.build_search_index([guide])
        entries = {entry["id"]: entry for entry in index["entries"]}

        self.assertEqual(entries["rated"]["rating"], 4.8)
        self.assertEqual(entries["rated"]["user_rating_count"], 321)
        self.assertIn("rating", entries["unrated"])
        self.assertIsNone(entries["unrated"]["rating"])
        self.assertIn("user_rating_count", entries["unrated"])
        self.assertIsNone(entries["unrated"]["user_rating_count"])

    def test_search_index_and_manifest_skip_permanently_closed_places(self) -> None:
        guide = Guide(
            slug="tokyo-japan",
            title="Tokyo, Japan",
            country_name="Japan",
            city_name="Tokyo",
            generated_at="2026-04-20T00:00:00+00:00",
            place_count=2,
            featured_place_ids=["closed", "temporary"],
            places=[
                NormalizedPlace(
                    id="closed",
                    name="Closed Cafe",
                    maps_url="https://maps.example/closed",
                    status="closed-permanently",
                ),
                NormalizedPlace(
                    id="temporary",
                    name="Temporary Tea",
                    maps_url="https://maps.example/temporary",
                    status="temporarily-closed",
                ),
                NormalizedPlace(
                    id="active",
                    name="Active Coffee",
                    maps_url="https://maps.example/active",
                    status="active",
                ),
            ],
        )

        manifest = build_data.summarize_guide(guide)
        index = build_data.build_search_index([guide])

        self.assertEqual(manifest.featured_names, ["Temporary Tea"])
        self.assertEqual(index["guides"][0]["featured_names"], ["Temporary Tea"])
        self.assertEqual([entry["id"] for entry in index["entries"]], ["temporary", "active"])

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

    def test_suppress_far_map_pins_clears_extreme_outlier_coordinates(self) -> None:
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

        with patch("builtins.print") as print_mock:
            build_data.suppress_far_map_pins("tokyo-japan", places, (35.665, 139.705))

        self.assertEqual((places[-1].lat, places[-1].lng), (None, None))
        suppression_warning = print_mock.call_args.args[0]
        self.assertIn("Suppressing map pin for tokyo-japan:bad-import", suppression_warning)

    def test_suppress_far_map_pins_keeps_non_extreme_outlier_coordinates(self) -> None:
        places = [
            NormalizedPlace(
                id="la-1",
                name="Los Angeles 1",
                lat=34.0522,
                lng=-118.2437,
                maps_url="https://maps.example/la-1",
                status="active",
            ),
            NormalizedPlace(
                id="la-2",
                name="Los Angeles 2",
                lat=34.0622,
                lng=-118.2537,
                maps_url="https://maps.example/la-2",
                status="active",
            ),
            NormalizedPlace(
                id="la-3",
                name="Los Angeles 3",
                lat=34.0722,
                lng=-118.2637,
                maps_url="https://maps.example/la-3",
                status="active",
            ),
            NormalizedPlace(
                id="far-but-possible",
                name="Far but possible",
                lat=34.0522,
                lng=-119.3537,
                maps_url="https://maps.example/far-but-possible",
                status="active",
            ),
        ]

        with patch("builtins.print") as print_mock:
            build_data.suppress_far_map_pins("los-angeles-california-usa", places, (34.0622, -118.2537))

        self.assertIsNotNone(places[-1].lat)
        self.assertIsNotNone(places[-1].lng)
        print_mock.assert_not_called()

    def test_suppress_far_map_pins_skips_sparse_guides(self) -> None:
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
                id="bad-import",
                name="Bad import",
                lat=48.86,
                lng=2.35,
                maps_url="https://maps.example/bad-import",
                status="active",
            ),
        ]

        with patch("builtins.print") as print_mock:
            build_data.suppress_far_map_pins(
                "tokyo-japan",
                places,
                build_data.guide_location_center(places),
            )

        self.assertEqual((places[-1].lat, places[-1].lng), (48.86, 2.35))
        print_mock.assert_not_called()

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
            (
                "106 台湾 Taipei City, Da’an District, Section 4, Zhongxiao E Rd, 250-4號1F",
                "Taipei",
                {"da-an-district", "taipei"},
                {"taiwan", "taipei-city", "zhongxiao-e-rd"},
                "Da’an District",
            ),
            (
                "No, No. 60, Section 2, Hankou St, Wanhua District, Taipei City, 108",
                "Taipei",
                {"wanhua-district", "taipei"},
                {"no", "taipei-city", "hankou-st"},
                "Wanhua District",
            ),
            (
                "Japan, 044-0081 Hokkaido, Abuta District, Kutchan, Yamada, 132-26",
                "Niseko",
                {"kutchan", "yamada", "niseko"},
                {"hokkaido"},
                "Kutchan",
            ),
            (
                "2 Chome-7-22 Asato, Naha, Okinawa 902-0067, Japan",
                "Okinawa Main Island",
                {"asato", "naha", "okinawa-main-island"},
                {"okinawa"},
                "Asato",
            ),
            (
                "C/ de Manso, 22, L'Eixample, 08015 Barcelona, Spain",
                "Barcelona",
                {"l-eixample", "barcelona"},
                {"c-de-manso"},
                "L'Eixample",
            ),
            (
                "Carrer de Blai, 47, Sants-Montjuïc, 08004 Barcelona, Spain",
                "Barcelona",
                {"sants-montjuic", "barcelona"},
                {"carrer-de-blai"},
                "Sants-Montjuïc",
            ),
            (
                "Plaça Comercial, 10, Ciutat Vella, 08003 Barcelona, Spain",
                "Barcelona",
                {"ciutat-vella", "barcelona"},
                {"placa-comercial"},
                "Ciutat Vella",
            ),
            (
                "La Rambla, 124, Ciutat Vella, 08002 Barcelona, スペイン",
                "Barcelona",
                {"ciutat-vella", "barcelona"},
                {"スペイン"},
                "Ciutat Vella",
            ),
            (
                "Av. del Paral·lel, 126 bis, Eixample, 08015 Barcelona, Spain",
                "Barcelona",
                {"eixample", "barcelona"},
                {"bis", "av-del-parallel"},
                "Eixample",
            ),
            (
                "Pl. de Carles Buïgas, 1, Sants-Montjuïc, 08038 Barcelona, Spain",
                "Barcelona",
                {"sants-montjuic", "barcelona"},
                {"pl-de-carles-buigas"},
                "Sants-Montjuïc",
            ),
            (
                "Moll dels Pescadors, 1, Barceloneta, Ciutat Vella, 08003 Barcelona, Spain",
                "Barcelona",
                {"barceloneta", "ciutat-vella", "barcelona"},
                {"moll-dels-pescadors"},
                "Barceloneta",
            ),
            (
                "Dentro del restaurante The Lobster Roll, Carrer del Rec Comtal, 12, Eixample, 08003 Barcelona, Spain",
                "Barcelona",
                {"eixample", "barcelona"},
                {"dentro-del-restaurante-the-lobster-roll"},
                "Eixample",
            ),
            (
                "Carrer de Marià Labèrnia, s/n, El Carmel, Horta-Guinardó, 08032 Barcelona, Spain",
                "Barcelona",
                {"el-carmel", "horta-guinardo", "barcelona"},
                {"s-n"},
                "El Carmel",
            ),
            (
                "144 Elizabeth St, Hobart TAS 7000, Australia",
                "Tasmania",
                {"hobart", "tasmania"},
                {"tas"},
                "Hobart",
            ),
            (
                "26-28 Cotham Rd, Kew VIC 3101, Australia",
                "Melbourne",
                {"kew", "melbourne"},
                {"vic"},
                "Kew",
            ),
            (
                "Council Pl, Sydney NSW 2000, Australia",
                "Sydney",
                {"sydney"},
                {"nsw", "council-pl"},
                None,
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
        self.assertEqual(
            place.maps_url,
            "https://www.google.com/maps/search/?api=1&query=Modern+Tea+House%2C+1+Songshan%2C+Taipei%2C+Taiwan",
        )
        self.assertEqual(place.provenance.name.source, "google_places")
        self.assertEqual(place.provenance.name.fetched_at, "2026-04-16T00:00:00+00:00")
        self.assertEqual(place.provenance.address.source, "google_places")
        self.assertEqual(place.provenance.maps_url.source, "google_places")
        self.assertEqual(place.provenance.primary_category.source, "google_places")
        self.assertEqual(
            {field.value: field.source for field in place.provenance.tags},
            {
                "cafe": "google_places",
                "taipei": "google_list",
                "tea-house": "google_places",
            },
        )

    def test_normalize_guide_prefers_place_id_search_url_when_available(self) -> None:
        raw = RawSavedList(
            title="Sydney, Australia",
            places=[
                RawPlace(
                    name="Cantina OK!",
                    address="Council Pl, Sydney NSW 2000, Australia",
                    lat=-33.8702175,
                    lng=151.2051413,
                    maps_url="https://maps.google.com/?cid=7715422616180689913",
                    cid="7715422616180689913",
                )
            ],
        )
        place_id = build_data.stable_place_id(raw.places[0])
        enrichment_cache = {
            place_id: EnrichmentCacheEntry(
                fetched_at="2026-04-16T00:00:00+00:00",
                query="Cantina OK!, Sydney",
                matched=True,
                score=88,
                place=EnrichmentPlace(
                    display_name="Cantina OK!",
                    formatted_address="Council Pl, Sydney NSW 2000, Australia",
                    google_maps_uri="https://maps.google.com/?cid=7715422616180689913",
                    google_place_id="ChIJGcmcg7ZC1moRAOacd3HoEwM",
                ),
            )
        }

        guide = build_data.normalize_guide("sydney-australia", raw, enrichment_cache=enrichment_cache)

        self.assertEqual(
            guide.places[0].maps_url,
            (
                "https://www.google.com/maps/search/?api=1"
                "&query=Cantina+OK%21%2C+Council+Pl%2C+Sydney+NSW+2000%2C+Australia"
                "&query_place_id=ChIJGcmcg7ZC1moRAOacd3HoEwM"
            ),
        )

    def test_normalize_guide_recomputes_center_after_pin_suppression(self) -> None:
        raw = RawSavedList(
            title="Tokyo, Japan",
            places=[
                RawPlace(
                    name="Tokyo 1",
                    address="1 Shibuya, Tokyo, Japan",
                    lat=35.66,
                    lng=139.70,
                    maps_url="https://maps.google.com/?cid=1",
                    cid="1",
                ),
                RawPlace(
                    name="Tokyo 2",
                    address="2 Shibuya, Tokyo, Japan",
                    lat=35.67,
                    lng=139.71,
                    maps_url="https://maps.google.com/?cid=2",
                    cid="2",
                ),
            ],
        )

        def suppress_and_mutate(
            _slug: str,
            places: list[NormalizedPlace],
            _center: tuple[float | None, float | None],
        ) -> None:
            places[-1].lat = None
            places[-1].lng = None

        with (
            patch.object(build_data, "suppress_far_map_pins", side_effect=suppress_and_mutate),
            patch.object(build_data, "warn_far_map_pins") as warn_mock,
        ):
            guide = build_data.normalize_guide("tokyo-japan", raw, enrichment_cache={})

        self.assertAlmostEqual(guide.center_lat or 0, 35.66, places=3)
        self.assertAlmostEqual(guide.center_lng or 0, 139.70, places=3)
        self.assertEqual(warn_mock.call_args.args[2], (guide.center_lat, guide.center_lng))

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
                build_data.release_scraper_session_lock(state)

                refreshed = build_data.ensure_scraper_session_state(None, now=fresh_now)

            self.assertTrue(refreshed.identity_dir.is_dir())
            self.assertFalse(refreshed.http_cookie_jar_path.exists())
            self.assertFalse(refreshed.browser_profile_dir.exists())
            build_data.release_scraper_session_lock(refreshed)

    def test_scraper_session_is_stale_without_metadata_when_artifacts_exist(self) -> None:
        with TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            with patch.object(build_data, "SCRAPER_STATE_DIR", state_dir):
                state = build_data.build_scraper_session_state(None)
                state.browser_profile_dir.mkdir(parents=True, exist_ok=True)

            self.assertTrue(build_data.scraper_session_is_stale(state))

    def test_build_scraper_session_state_uses_stable_slot_state_files(self) -> None:
        state = build_data.build_scraper_session_state("http://proxy.example:8080")
        self.assertEqual(state.slot_key, "slot-0")
        self.assertEqual(state.browser_profile_dir.name, "slot-0")
        self.assertEqual(state.http_cookie_jar_path.name, "http-cookies.slot-0.txt")
        self.assertEqual(state.metadata_path.name, "metadata.slot-0.json")

    def test_ensure_scraper_session_state_reuses_stable_slot_files_after_lock_release(self) -> None:
        with TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            now = datetime(2026, 4, 1, tzinfo=UTC)

            with patch.object(build_data, "SCRAPER_STATE_DIR", state_dir):
                first = build_data.ensure_scraper_session_state(None, now=now)
                build_data.record_scraper_session_use(first, proxy=None, now=now)
                build_data.release_scraper_session_lock(first)

                second = build_data.ensure_scraper_session_state(None, now=now + timedelta(hours=1))

            self.assertEqual(second.slot_key, first.slot_key)
            self.assertEqual(second.browser_profile_dir, first.browser_profile_dir)
            self.assertEqual(second.http_cookie_jar_path, first.http_cookie_jar_path)
            self.assertEqual(second.metadata_path, first.metadata_path)
            build_data.release_scraper_session_lock(second)

    def test_clear_scraper_session_state_keeps_other_slot_files(self) -> None:
        with TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            with patch.object(build_data, "SCRAPER_STATE_DIR", state_dir):
                state = build_data.build_scraper_session_state(None)
                sibling_browser_dir = state.identity_dir / "browser" / "slot-1"
                sibling_cookie_jar_path = state.identity_dir / "http-cookies.slot-1.txt"
                sibling_metadata_path = state.identity_dir / "metadata.slot-1.json"

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

    def test_ensure_scraper_session_state_sweeps_stale_sibling_slot_state(self) -> None:
        with TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            stale_now = datetime(2026, 4, 1, tzinfo=UTC)
            fresh_now = stale_now + build_data.SCRAPER_SESSION_MAX_AGE + timedelta(minutes=1)

            with patch.object(build_data, "SCRAPER_STATE_DIR", state_dir):
                stale_state = build_data.build_scraper_session_state(None, slot_key="slot-1")
                stale_state.browser_profile_dir.mkdir(parents=True, exist_ok=True)
                stale_state.http_cookie_jar_path.write_text("cookie", encoding="utf-8")
                build_data.record_scraper_session_use(stale_state, proxy=None, now=stale_now)

                current_state = build_data.ensure_scraper_session_state(None, now=fresh_now)

            self.assertFalse(stale_state.browser_profile_dir.exists())
            self.assertFalse(stale_state.http_cookie_jar_path.exists())
            self.assertFalse(stale_state.metadata_path.exists())
            build_data.release_scraper_session_lock(current_state)

    def test_scraper_session_lock_is_active_for_new_unreadable_lock(self) -> None:
        with TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "slot-0.lock"
            lock_path.write_text("", encoding="utf-8")

            self.assertTrue(build_data.scraper_session_lock_is_active(lock_path))

    def test_scraper_session_lock_is_inactive_for_old_unreadable_lock(self) -> None:
        with TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "slot-0.lock"
            lock_path.write_text("", encoding="utf-8")
            old_timestamp = (
                datetime.now(UTC) - build_data.SCRAPER_SESSION_LOCK_WRITE_GRACE - timedelta(seconds=1)
            ).timestamp()
            os.utime(lock_path, (old_timestamp, old_timestamp))

            self.assertFalse(build_data.scraper_session_lock_is_active(lock_path))

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
        self.assertEqual(kwargs["collection_mode"], "curl")
        self.assertEqual(kwargs["browser_session"].proxy, "http://proxy.example:8080")
        self.assertEqual(kwargs["http_session"].proxy, "http://proxy.example:8080")
        self.assertIn("proxy-", str(kwargs["browser_session"].profile_dir))
        self.assertIn("proxy-", str(kwargs["http_session"].cookie_jar_path))

    def test_scrape_google_list_url_clears_session_after_curl_block_and_falls_back_to_browser(self) -> None:
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
        self.assertEqual(scrape.call_args_list[0].kwargs["collection_mode"], "curl")
        self.assertEqual(scrape.call_args_list[1].kwargs["collection_mode"], "browser")
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
            entry = build_data.fetch_places_enrichment(
                place,
                api_key=None,
                strategy="scrape_then_api",
            )

        scrape.assert_called_once()
        self.assertEqual(entry.source, "google_maps_page")
        self.assertTrue(entry.matched)
        self.assertEqual(entry.place.display_name, "Jimbocho Den")
        self.assertEqual(entry.place.primary_type, "japanese_restaurant")
        self.assertEqual(entry.place.user_rating_count, 324)
        self.assertEqual(entry.place.business_status, "OPERATIONAL")

    def test_fetch_place_page_enrichment_rewrites_cid_url_before_scraping(self) -> None:
        place = RawPlace(
            name="Cantina OK!",
            address="Council Pl, Sydney NSW 2000, Australia",
            lat=-33.8702175,
            lng=151.2051413,
            maps_url="https://maps.google.com/?cid=7715422616180689913",
            cid="7715422616180689913",
        )
        details = SimpleNamespace(
            source_url=(
                "https://www.google.com/maps/search/?api=1"
                "&query=Cantina+OK%21%2C+Council+Pl%2C+Sydney+NSW+2000%2C+Australia"
            ),
            resolved_url="https://www.google.com/maps/place/Cantina+OK!/@-33.8702175,151.2051413,17z",
            name="Cantina OK!",
            category="Cocktail bar",
            address="Council Pl, Sydney NSW 2000, Australia",
            status="Open",
            limited_view=False,
        )

        with (
            patch.object(build_data, "current_scraper_proxy", return_value=None),
            patch.object(build_data, "build_scraper_sessions", return_value=(SimpleNamespace(), None, None)),
            patch.object(build_data, "record_scraper_session_use"),
            patch.object(build_data, "release_scraper_session_lock"),
            patch.object(build_data, "scrape_place", return_value=details) as scrape,
        ):
            entry = build_data.fetch_place_page_enrichment(place)

        scrape.assert_called_once_with(
            (
                "https://www.google.com/maps/search/?api=1"
                "&query=Cantina+OK%21%2C+Council+Pl%2C+Sydney+NSW+2000%2C+Australia"
                "&hl=en&gl=us"
            ),
            headless=True,
            browser_session=None,
            http_session=None,
        )
        self.assertTrue(entry.matched)
        self.assertEqual(entry.place.display_name, "Cantina OK!")

    def test_fetch_places_enrichment_uses_scrape_strategy_without_api_fallback(self) -> None:
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

        with (
            patch.object(build_data, "fetch_place_page_enrichment", return_value=page_entry) as page_fetch,
            patch.object(build_data, "fetch_places_api_enrichment") as api_fetch,
        ):
            entry = build_data.fetch_places_enrichment(
                place,
                api_key="test-key",
                strategy="scrape",
            )

        page_fetch.assert_called_once_with(
            place,
            city_name=None,
            country_name=None,
            google_place_id=None,
        )
        api_fetch.assert_not_called()
        self.assertIs(entry, page_entry)

    def test_fetch_places_enrichment_uses_api_strategy_without_scraping(self) -> None:
        place = RawPlace(
            name="Jimbocho Den",
            address="Tokyo, Japan",
            maps_url="https://www.google.com/maps/place/Jimbocho+Den",
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
            patch.object(build_data, "fetch_place_page_enrichment") as page_fetch,
            patch.object(build_data, "fetch_places_api_enrichment", return_value=api_entry) as api_fetch,
        ):
            entry = build_data.fetch_places_enrichment(
                place,
                api_key="test-key",
                strategy="api",
            )

        page_fetch.assert_not_called()
        api_fetch.assert_called_once_with(
            place,
            city_name=None,
            country_name=None,
            api_key="test-key",
        )
        self.assertIs(entry, api_entry)

    def test_fetch_places_enrichment_fails_for_api_strategy_without_key(self) -> None:
        place = RawPlace(
            name="Jimbocho Den",
            address="Tokyo, Japan",
            maps_url="https://www.google.com/maps/place/Jimbocho+Den",
        )

        with (
            patch.object(build_data, "fetch_place_page_enrichment") as page_fetch,
            patch.object(build_data, "fetch_places_api_enrichment") as api_fetch,
        ):
            with self.assertRaisesRegex(RuntimeError, "GOOGLE_PLACES_API_KEY is required"):
                build_data.fetch_places_enrichment(
                    place,
                    api_key=None,
                    strategy="api",
                )

        page_fetch.assert_not_called()
        api_fetch.assert_not_called()

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
            entry = build_data.fetch_places_enrichment(
                place,
                api_key="test-key",
                strategy="scrape_then_api",
            )

        page_fetch.assert_called_once_with(
            place,
            city_name=None,
            country_name=None,
            google_place_id=None,
        )
        api_fetch.assert_called_once_with(
            place,
            city_name=None,
            country_name=None,
            api_key="test-key",
        )
        self.assertIs(entry, api_entry)

    def test_fetch_places_enrichment_keeps_page_result_when_api_fallback_fails(self) -> None:
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
            refresh_after="2026-04-17T00:00:00+00:00",
            source="google_places_api",
            query="Jimbocho Den, Tokyo, Japan",
            matched=False,
            error="http_error:403",
        )

        with (
            patch.object(build_data, "fetch_place_page_enrichment", return_value=page_entry) as page_fetch,
            patch.object(build_data, "fetch_places_api_enrichment", return_value=api_entry) as api_fetch,
        ):
            entry = build_data.fetch_places_enrichment(
                place,
                api_key="test-key",
                strategy="scrape_then_api",
            )

        page_fetch.assert_called_once_with(
            place,
            city_name=None,
            country_name=None,
            google_place_id=None,
        )
        api_fetch.assert_called_once_with(
            place,
            city_name=None,
            country_name=None,
            api_key="test-key",
        )
        self.assertIs(entry, page_entry)

    def test_normalize_place_page_enrichment_omits_maps_uri_for_url_only_result(self) -> None:
        details = SimpleNamespace(
            source_url="https://www.google.com/maps/place/Jimbocho+Den",
            resolved_url="https://www.google.com/maps/place/Jimbocho+Den/@35.67,139.71,17z",
            limited_view=False,
        )

        enrichment_place = build_data.normalize_place_page_enrichment(details)

        self.assertIsNone(enrichment_place.google_maps_uri)
        self.assertFalse(build_data.place_page_has_meaningful_enrichment(details, enrichment_place))

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

                def submit(self, _fn, source, **kwargs):
                    test_case.assertEqual(kwargs["refresh_retries"], build_data.DEFAULT_REFRESH_RETRIES)
                    test_case.assertFalse(kwargs["headed"])
                    return futures_by_slug[source.slug]

                def shutdown(self, wait=True, cancel_futures=False):
                    return None

            with (
                patch.object(build_data, "RAW_DIR", raw_dir),
                patch.object(build_data, "load_sources", return_value=[first_source, second_source]),
                patch.object(build_data, "ThreadPoolExecutor", FakeExecutor),
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

    def test_parallel_refresh_terminates_workers_on_interrupt(self) -> None:
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

        class FakeFuture:
            def result(self):
                raise KeyboardInterrupt()

        future = FakeFuture()
        executor_holder: dict[str, object] = {}

        class FakeExecutor:
            def __init__(self, max_workers):
                self.max_workers = max_workers
                self.terminated = False
                self.shutdown_calls: list[tuple[bool, bool]] = []
                executor_holder["executor"] = self

            def submit(self, _fn, source, **kwargs):
                return future

            def terminate_workers(self):
                self.terminated = True

            def shutdown(self, wait=True, cancel_futures=False):
                self.shutdown_calls.append((wait, cancel_futures))

        with (
            patch.object(build_data, "load_sources", return_value=[first_source, second_source]),
            patch.object(build_data, "cache_refresh_reason", return_value="forced"),
            patch.object(build_data, "ThreadPoolExecutor", FakeExecutor),
            patch.object(build_data, "as_completed", return_value=[future]),
            patch("builtins.print"),
        ):
            with self.assertRaises(KeyboardInterrupt):
                build_data.refresh_raw_sources(
                    headed=False,
                    force_refresh=True,
                    refresh_lists=[],
                    refresh_workers=2,
                    refresh_startup_jitter_seconds=0,
                )

        executor = executor_holder["executor"]
        assert isinstance(executor, FakeExecutor)
        self.assertTrue(executor.terminated)
        self.assertIn((True, True), executor.shutdown_calls)

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

    def test_enrich_raw_sources_missing_only_skips_expired_entries(self) -> None:
        raw = RawSavedList(
            title="Tokyo",
            places=[
                RawPlace(name="Expired Place", maps_url="https://maps.google.com/?cid=111", cid="111"),
                RawPlace(name="Missing Place", maps_url="https://maps.google.com/?cid=222", cid="222"),
            ],
        )

        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            raw_dir = tmpdir_path / "raw"
            cache_dir = tmpdir_path / "cache"
            db_path = tmpdir_path / "places.sqlite"
            raw_dir.mkdir()
            cache_dir.mkdir()
            (raw_dir / "tokyo-japan.json").write_text(
                json.dumps(raw.model_dump(mode="json"), ensure_ascii=False),
                encoding="utf-8",
            )

            seen_reasons: list[str] = []

            def fake_enrich_place_job(_slug, _place_id, _place_name, refresh_reason, _place_payload, **_kwargs):
                seen_reasons.append(refresh_reason)
                return EnrichmentCacheEntry(
                    fetched_at="2026-04-20T00:00:00+00:00",
                    query="Missing Place, Tokyo",
                    matched=True,
                    place=EnrichmentPlace(display_name="Missing Place"),
                )

            with (
                patch.object(build_data, "RAW_DIR", raw_dir),
                patch.object(build_data, "PLACES_CACHE_DIR", cache_dir),
                patch.object(build_data, "PLACES_SQLITE_PATH", db_path),
                patch.object(build_data, "google_places_api_key", return_value=None),
                patch.object(
                    build_data,
                    "cache_refresh_reason",
                    side_effect=lambda place, _cache_entry, **_kwargs: {
                        "Expired Place": "refresh-window-expired",
                        "Missing Place": "missing-cache-entry",
                    }[place.name],
                ),
                patch.object(build_data, "enrich_place_job", side_effect=fake_enrich_place_job),
            ):
                build_data.enrich_raw_sources(
                    force_refresh=False,
                    missing_only=True,
                    refresh_workers=1,
                    refresh_startup_jitter_seconds=0,
                )

            self.assertEqual(seen_reasons, ["missing-cache-entry"])

    def test_enrich_raw_sources_prioritizes_missing_entries_before_expired_refreshes(self) -> None:
        raw = RawSavedList(
            title="Tokyo",
            places=[
                RawPlace(name="Expired Place", maps_url="https://maps.google.com/?cid=111", cid="111"),
                RawPlace(name="Missing Place", maps_url="https://maps.google.com/?cid=222", cid="222"),
            ],
        )

        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            raw_dir = tmpdir_path / "raw"
            cache_dir = tmpdir_path / "cache"
            db_path = tmpdir_path / "places.sqlite"
            raw_dir.mkdir()
            cache_dir.mkdir()
            (raw_dir / "tokyo-japan.json").write_text(
                json.dumps(raw.model_dump(mode="json"), ensure_ascii=False),
                encoding="utf-8",
            )

            seen_reasons: list[str] = []

            def fake_enrich_place_job(_slug, _place_id, _place_name, refresh_reason, _place_payload, **_kwargs):
                seen_reasons.append(refresh_reason)
                return EnrichmentCacheEntry(
                    fetched_at="2026-04-20T00:00:00+00:00",
                    query="Tokyo",
                    matched=True,
                    place=EnrichmentPlace(display_name="Tokyo"),
                )

            with (
                patch.object(build_data, "RAW_DIR", raw_dir),
                patch.object(build_data, "PLACES_CACHE_DIR", cache_dir),
                patch.object(build_data, "PLACES_SQLITE_PATH", db_path),
                patch.object(build_data, "google_places_api_key", return_value=None),
                patch.object(
                    build_data,
                    "cache_refresh_reason",
                    side_effect=lambda place, _cache_entry, **_kwargs: {
                        "Expired Place": "refresh-window-expired",
                        "Missing Place": "missing-cache-entry",
                    }[place.name],
                ),
                patch.object(build_data, "enrich_place_job", side_effect=fake_enrich_place_job),
            ):
                build_data.enrich_raw_sources(
                    force_refresh=False,
                    missing_only=False,
                    refresh_workers=1,
                    refresh_startup_jitter_seconds=0,
                )

            self.assertEqual(
                seen_reasons,
                ["missing-cache-entry", "refresh-window-expired"],
            )

    def test_enrich_raw_sources_parallel_writes_cache_incrementally(self) -> None:
        raw = RawSavedList(
            title="Tokyo",
            places=[
                RawPlace(
                    name="First Place",
                    address="1 Example St, Tokyo",
                    maps_url="https://maps.google.com/?cid=111",
                    cid="111",
                ),
                RawPlace(
                    name="Second Place",
                    address="2 Example St, Tokyo",
                    maps_url="https://maps.google.com/?cid=222",
                    cid="222",
                ),
            ],
        )

        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            raw_dir = tmpdir_path / "raw"
            cache_dir = tmpdir_path / "cache"
            db_path = tmpdir_path / "places.sqlite"
            raw_dir.mkdir()
            cache_dir.mkdir()
            (raw_dir / "tokyo-japan.json").write_text(
                json.dumps(raw.model_dump(mode="json"), ensure_ascii=False),
                encoding="utf-8",
            )

            first_place_id = build_data.stable_place_id(raw.places[0])
            second_place_id = build_data.stable_place_id(raw.places[1])

            class FakeFuture:
                def __init__(self, result):
                    self._result = result

                def result(self):
                    return self._result()

            first_future = FakeFuture(
                lambda: EnrichmentCacheEntry(
                    fetched_at="2026-04-20T00:00:00+00:00",
                    query="First Place, Tokyo",
                    matched=True,
                    place=EnrichmentPlace(display_name="First Place"),
                )
            )

            def second_result() -> EnrichmentCacheEntry:
                cache_payload = build_data.load_places_cache("tokyo-japan")
                self.assertIn(first_place_id, cache_payload)
                self.assertNotIn(second_place_id, cache_payload)
                return EnrichmentCacheEntry(
                    fetched_at="2026-04-20T00:00:01+00:00",
                    query="Second Place, Tokyo",
                    matched=True,
                    place=EnrichmentPlace(display_name="Second Place"),
                )

            second_future = FakeFuture(second_result)
            futures_by_cid = {
                "111": first_future,
                "222": second_future,
            }
            test_case = self

            class FakeExecutor:
                def __init__(self, max_workers):
                    test_case.assertEqual(max_workers, 2)

                def submit(self, _fn, _slug, _place_id, _place_name, _refresh_reason, place_payload, **kwargs):
                    test_case.assertIsNone(kwargs["api_key"])
                    test_case.assertEqual(kwargs["refresh_startup_jitter_seconds"], 0)
                    return futures_by_cid[place_payload["cid"]]

                def shutdown(self, wait=True, cancel_futures=False):
                    return None

            with (
                patch.object(build_data, "RAW_DIR", raw_dir),
                patch.object(build_data, "PLACES_CACHE_DIR", cache_dir),
                patch.object(build_data, "PLACES_SQLITE_PATH", db_path),
                patch.object(build_data, "google_places_api_key", return_value=None),
                patch.object(build_data, "ThreadPoolExecutor", FakeExecutor),
                patch.object(build_data, "as_completed", return_value=[first_future, second_future]),
            ):
                build_data.enrich_raw_sources(
                    force_refresh=True,
                    refresh_workers=2,
                    refresh_startup_jitter_seconds=0,
                )

                cache_payload = build_data.load_places_cache("tokyo-japan")
            self.assertIn(first_place_id, cache_payload)
            self.assertIn(second_place_id, cache_payload)
            self.assertEqual(cache_payload[first_place_id].place.display_name, "First Place")
            self.assertEqual(cache_payload[second_place_id].place.display_name, "Second Place")

    def test_place_input_signature_prefers_stable_identifiers(self) -> None:
        first_place = RawPlace(
            name="Old Name",
            address="Old Address",
            maps_url="https://maps.google.com/?cid=111",
            cid="111",
            lat=1.0,
            lng=2.0,
        )
        second_place = RawPlace(
            name="New Name",
            address="New Address",
            maps_url="https://www.google.com/maps/search/?api=1&query=New+Name",
            cid="111",
            lat=9.0,
            lng=10.0,
        )

        self.assertEqual(
            build_data.place_input_signature(first_place),
            build_data.place_input_signature(second_place),
        )

    def test_enrichment_input_signature_includes_locality_bias_context(self) -> None:
        place = RawPlace(
            name="Bilmonte",
            address=None,
            maps_url="https://www.google.com/maps/search/?api=1&query=Bilmonte",
            cid="1343378048703211865",
            lat=41.3894089,
            lng=2.1636435,
        )

        generic_signature = build_data.enrichment_input_signature(place)
        barcelona_signature = build_data.enrichment_input_signature(
            place,
            city_name="Barcelona",
            country_name="Spain",
        )
        madrid_signature = build_data.enrichment_input_signature(
            place,
            city_name="Madrid",
            country_name="Spain",
        )

        self.assertNotEqual(generic_signature, barcelona_signature)
        self.assertNotEqual(barcelona_signature, madrid_signature)

    def test_cache_refresh_reason_invalidates_legacy_unbiased_name_only_search_entry(self) -> None:
        place = RawPlace(
            name="Bilmonte",
            address=None,
            maps_url="https://www.google.com/maps/search/?api=1&query=Bilmonte",
            cid="1343378048703211865",
            lat=41.3894089,
            lng=2.1636435,
        )
        cache_entry = EnrichmentCacheEntry(
            fetched_at="2026-04-19T16:47:26.628975+00:00",
            refresh_after="2026-05-19T16:47:26.628975+00:00",
            source="google_maps_page",
            query="Bilmonte",
            input_signature=build_data.place_input_signature(place),
            matched=True,
            score=build_data.STRONG_MATCH_SCORE,
            place=EnrichmentPlace(
                display_name="Bilmonte",
                formatted_address="30 Great Windmill St, London W1D 7LW, United Kingdom",
            ),
        )

        refresh_reason = build_data.cache_refresh_reason(
            place,
            cache_entry,
            city_name="Barcelona",
            country_name="Spain",
        )

        self.assertEqual(refresh_reason, "raw-place-changed")

    def test_build_places_sqlite_signature_changes_when_version_or_schema_changes(self) -> None:
        raw = RawSavedList(
            configured_source_type="google_list_url",
            fetched_at="2026-04-20T00:00:00+00:00",
            title="Tokyo",
            places=[
                RawPlace(
                    name="Coffee House",
                    address="1 Shibuya, Tokyo, Japan",
                    maps_url="https://maps.google.com/?cid=111",
                    cid="111",
                )
            ],
        )
        place_id = build_data.stable_place_id(raw.places[0], source_type=raw.configured_source_type)
        guide = Guide(
            slug="tokyo-japan",
            title="Tokyo",
            country_name="Japan",
            city_name="Tokyo",
            generated_at="2026-04-20T00:00:00+00:00",
            place_count=1,
            places=[
                NormalizedPlace(
                    id=place_id,
                    name="Coffee House",
                    maps_url="https://www.google.com/maps/search/?api=1&query=Coffee+House",
                    status="active",
                )
            ],
        )
        baseline = build_data.build_places_sqlite_signature(
            raw_lists={"tokyo-japan": raw},
            guides=[guide],
            enrichment_caches={"tokyo-japan": {}},
        )

        with patch.object(build_data, "PLACES_SQLITE_SIGNATURE_VERSION", build_data.PLACES_SQLITE_SIGNATURE_VERSION + 1):
            bumped_version = build_data.build_places_sqlite_signature(
                raw_lists={"tokyo-japan": raw},
                guides=[guide],
                enrichment_caches={"tokyo-japan": {}},
            )

        with patch.object(build_data, "PLACES_SQLITE_SCHEMA_SQL", build_data.PLACES_SQLITE_SCHEMA_SQL + "\n-- test change"):
            changed_schema = build_data.build_places_sqlite_signature(
                raw_lists={"tokyo-japan": raw},
                guides=[guide],
                enrichment_caches={"tokyo-japan": {}},
            )

        self.assertNotEqual(baseline, bumped_version)
        self.assertNotEqual(baseline, changed_schema)

    def test_place_photo_signature_state_tracks_photo_digest_not_mtime(self) -> None:
        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            photo_path = tmpdir_path / "public" / "place-photos" / "cid-111-photo.webp"
            photo_path.parent.mkdir(parents=True, exist_ok=True)
            first_bytes = b"first-photo"
            second_bytes = b"other-photo"
            fixed_mtime = 1_700_000_000

            photo_path.write_bytes(first_bytes)
            os.utime(photo_path, (fixed_mtime, fixed_mtime))

            with patch.object(build_data, "ROOT", tmpdir_path):
                first_state = build_data.place_photo_signature_state(
                    "/place-photos/cid-111-photo.webp",
                    source_photo_url="https://images.example/coffee-house.webp",
                    metadata_cache={},
                )

                photo_path.write_bytes(second_bytes)
                os.utime(photo_path, (fixed_mtime, fixed_mtime))

                second_state = build_data.place_photo_signature_state(
                    "/place-photos/cid-111-photo.webp",
                    source_photo_url="https://images.example/coffee-house.webp",
                    metadata_cache={},
                )

            self.assertNotEqual(first_state, second_state)
            self.assertEqual(first_state[1], hashlib.sha256(first_bytes).hexdigest())
            self.assertEqual(second_state[1], hashlib.sha256(second_bytes).hexdigest())

    def test_rebuild_places_sqlite_mirrors_cache_and_photo_metadata(self) -> None:
        raw = RawSavedList(
            configured_source_type="google_list_url",
            title="Tokyo",
            places=[
                RawPlace(
                    name="Coffee House",
                    address="1 Shibuya, Tokyo, Japan",
                    maps_url="https://maps.google.com/?cid=111",
                    cid="111",
                    lat=35.6595,
                    lng=139.7005,
                )
            ],
        )
        place_id = build_data.stable_place_id(raw.places[0], source_type=raw.configured_source_type)
        photo_public_path = "/place-photos/cid-111-photo.webp"
        photo_bytes = b"photo-bytes-for-sqlite"
        cache_entry = EnrichmentCacheEntry(
            fetched_at="2026-04-20T00:00:00+00:00",
            last_verified_at="2026-04-20T00:00:00+00:00",
            refresh_after="2026-04-27T00:00:00+00:00",
            source="google_maps_page",
            query="Coffee House, 1 Shibuya, Tokyo, Japan",
            input_signature=build_data.place_input_signature(raw.places[0]),
            matched=True,
            score=build_data.STRONG_MATCH_SCORE,
            place=EnrichmentPlace(
                google_place_id="ChIJ123",
                display_name="Coffee House",
                formatted_address="1 Shibuya, Tokyo, Japan",
                google_maps_uri="https://www.google.com/maps/place/Coffee+House",
                rating=4.8,
                user_rating_count=240,
                primary_type="cafe",
                primary_type_display_name="Cafe",
                types=["cafe", "food"],
                main_photo_url="https://images.example/coffee-house.webp",
                photo_url="https://images.example/coffee-house.webp",
            ),
        )
        guide = Guide(
            slug="tokyo-japan",
            title="Tokyo",
            country_name="Japan",
            city_name="Tokyo",
            generated_at="2026-04-20T00:00:00+00:00",
            place_count=1,
            featured_place_ids=[place_id],
            best_hit_place_ids=[place_id],
            top_categories=["cafe"],
            places=[
                NormalizedPlace(
                    id=place_id,
                    name="Coffee House",
                    address="1 Shibuya, Tokyo, Japan",
                    lat=35.6595,
                    lng=139.7005,
                    maps_url="https://www.google.com/maps/search/?api=1&query=Coffee+House",
                    cid="111",
                    google_place_id="ChIJ123",
                    rating=4.8,
                    user_rating_count=240,
                    primary_category="cafe",
                    tags=["coffee"],
                    vibe_tags=["cozy"],
                    neighborhood="Shibuya",
                    note="Great espresso",
                    why_recommended="Morning stop",
                    main_photo_path=photo_public_path,
                    top_pick=True,
                    hidden=False,
                    manual_rank=3,
                    status="active",
                )
            ],
        )

        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            db_path = tmpdir_path / "cache" / "places.sqlite"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            photo_path = tmpdir_path / "public" / photo_public_path.lstrip("/")
            photo_path.parent.mkdir(parents=True, exist_ok=True)
            photo_path.write_bytes(photo_bytes)

            with (
                patch.object(build_data, "ROOT", tmpdir_path),
                patch.object(build_data, "PLACES_SQLITE_PATH", db_path),
            ):
                build_data.rebuild_places_sqlite(
                    raw_lists={"tokyo-japan": raw},
                    guides=[guide],
                    enrichment_caches={"tokyo-japan": {place_id: cache_entry}},
                )

            connection = sqlite3.connect(db_path)
            try:
                canonical_place = connection.execute(
                    """
                    SELECT normalized_name, enrichment_display_name, enrichment_google_place_id,
                           normalized_main_photo_path
                    FROM canonical_places
                    WHERE place_id = ?
                    """,
                    (place_id,),
                ).fetchone()
                self.assertEqual(
                    canonical_place,
                    ("Coffee House", "Coffee House", "ChIJ123", photo_public_path),
                )

                sqlite_cache_entry = connection.execute(
                    """
                    SELECT query, input_signature, cache_json
                    FROM guide_enrichment_cache
                    WHERE guide_slug = ? AND place_id = ?
                    """,
                    ("tokyo-japan", place_id),
                ).fetchone()
                self.assertIsNotNone(sqlite_cache_entry)
                assert sqlite_cache_entry is not None
                self.assertEqual(
                    sqlite_cache_entry[:2],
                    (
                        "Coffee House, 1 Shibuya, Tokyo, Japan",
                        build_data.place_input_signature(raw.places[0]),
                    ),
                )
                self.assertEqual(
                    json.loads(sqlite_cache_entry[2])["place"]["display_name"],
                    "Coffee House",
                )
                build_metadata = connection.execute(
                    "SELECT value FROM build_metadata WHERE key = ?",
                    (build_data.PLACES_SQLITE_BUILD_METADATA_KEY,),
                ).fetchone()
                self.assertIsNotNone(build_metadata)
                assert build_metadata is not None
                self.assertRegex(build_metadata[0], r"^[0-9a-f]{64}$")

                guide_place = connection.execute(
                    """
                    SELECT guide_slug, is_featured, is_best_hit, main_photo_path
                    FROM guide_places
                    WHERE guide_slug = ? AND place_id = ?
                    """,
                    ("tokyo-japan", place_id),
                ).fetchone()
                self.assertEqual(
                    guide_place,
                    ("tokyo-japan", 1, 1, photo_public_path),
                )

                place_photo = connection.execute(
                    """
                    SELECT fetch_status, local_path, content_sha256, size_bytes
                    FROM place_photos
                    WHERE guide_slug = ? AND place_id = ?
                    """,
                    ("tokyo-japan", place_id),
                ).fetchone()
                self.assertEqual(
                    place_photo,
                    (
                        "downloaded",
                        photo_public_path,
                        hashlib.sha256(photo_bytes).hexdigest(),
                        len(photo_bytes),
                    ),
                )
            finally:
                connection.close()

    def test_rebuild_places_sqlite_hydrates_matching_guide_cache_photo_urls(self) -> None:
        shared_place_id = "cid:111"
        photo_url = "https://images.example/shared-photo.webp"
        kanazawa_raw = RawSavedList(
            configured_source_type="google_list_url",
            title="Kanazawa",
            places=[
                RawPlace(
                    name="Shared Place",
                    address="1 Kanazawa, Japan",
                    maps_url="https://maps.google.com/?cid=111",
                    cid="111",
                )
            ],
        )
        toyama_raw = RawSavedList(
            configured_source_type="google_list_url",
            title="Toyama",
            places=[
                RawPlace(
                    name="Shared Place",
                    address="1 Toyama, Japan",
                    maps_url="https://maps.google.com/?cid=111",
                    cid="111",
                )
            ],
        )
        kanazawa_guide = Guide(
            slug="kanazawa-japan",
            title="Kanazawa",
            country_name="Japan",
            city_name="Kanazawa",
            generated_at="2026-04-20T00:00:00+00:00",
            place_count=1,
            places=[
                NormalizedPlace(
                    id=shared_place_id,
                    name="Shared Place",
                    maps_url="https://maps.google.com/?cid=111",
                    cid="111",
                    google_place_id="place-123",
                    google_place_resource_name="places/place-123",
                    status="active",
                )
            ],
        )
        toyama_guide = Guide(
            slug="toyama-japan",
            title="Toyama",
            country_name="Japan",
            city_name="Toyama",
            generated_at="2026-04-20T00:00:00+00:00",
            place_count=1,
            places=[
                NormalizedPlace(
                    id=shared_place_id,
                    name="Shared Place",
                    maps_url="https://maps.google.com/?cid=111",
                    cid="111",
                    google_place_id="place-123",
                    google_place_resource_name="places/place-123",
                    main_photo_path="/place-photos/shared-photo.webp",
                    status="active",
                )
            ],
        )
        kanazawa_cache_entry = EnrichmentCacheEntry(
            fetched_at="2026-04-20T00:00:00+00:00",
            query="Shared Place, Kanazawa",
            matched=True,
            place=EnrichmentPlace(
                google_place_id="place-123",
                google_place_resource_name="places/place-123",
                display_name="Shared Place",
            ),
        )
        toyama_cache_entry = EnrichmentCacheEntry(
            fetched_at="2026-04-21T00:00:00+00:00",
            query="Shared Place, Toyama",
            matched=True,
            place=EnrichmentPlace(
                display_name="Shared Place",
                main_photo_url=photo_url,
                photo_url=photo_url,
            ),
        )

        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            db_path = tmpdir_path / "cache" / "places.sqlite"
            db_path.parent.mkdir(parents=True, exist_ok=True)

            with (
                patch.object(build_data, "ROOT", tmpdir_path),
                patch.object(build_data, "PLACES_SQLITE_PATH", db_path),
            ):
                build_data.rebuild_places_sqlite(
                    raw_lists={
                        "kanazawa-japan": kanazawa_raw,
                        "toyama-japan": toyama_raw,
                    },
                    guides=[kanazawa_guide, toyama_guide],
                    enrichment_caches={
                        "kanazawa-japan": {shared_place_id: kanazawa_cache_entry},
                        "toyama-japan": {shared_place_id: toyama_cache_entry},
                    },
                )

            connection = sqlite3.connect(db_path)
            try:
                sqlite_cache_entry = connection.execute(
                    """
                    SELECT cache_json
                    FROM guide_enrichment_cache
                    WHERE guide_slug = ? AND place_id = ?
                    """,
                    ("kanazawa-japan", shared_place_id),
                ).fetchone()
                self.assertIsNotNone(sqlite_cache_entry)
                assert sqlite_cache_entry is not None
                place_payload = json.loads(sqlite_cache_entry[0])["place"]
                self.assertEqual(place_payload["main_photo_url"], photo_url)
                self.assertEqual(place_payload["photo_url"], photo_url)
            finally:
                connection.close()

    def test_build_places_sqlite_rows_keeps_identified_canonical_row_over_unidentified_collision(self) -> None:
        shared_place_id = "cid:222"
        oita_raw = RawSavedList(
            configured_source_type="google_list_url",
            title="Oita",
            places=[
                RawPlace(
                    name="Milch",
                    address="1 Oita, Japan",
                    maps_url="https://maps.google.com/?cid=222",
                    cid="222",
                )
            ],
        )
        yufuin_raw = RawSavedList(
            configured_source_type="google_list_url",
            title="Yufuin",
            places=[
                RawPlace(
                    name="Share",
                    address="1 Yufuin, Japan",
                    maps_url="https://maps.google.com/?cid=222",
                    cid="222",
                )
            ],
        )
        oita_guide = Guide(
            slug="oita-japan",
            title="Oita",
            country_name="Japan",
            city_name="Oita",
            generated_at="2026-04-20T00:00:00+00:00",
            place_count=1,
            places=[
                NormalizedPlace(
                    id=shared_place_id,
                    name="Milch",
                    maps_url="https://maps.google.com/?cid=222",
                    cid="222",
                    google_place_id="place-milch",
                    google_place_resource_name="places/place-milch",
                    status="active",
                )
            ],
        )
        yufuin_guide = Guide(
            slug="yufuin-japan",
            title="Yufuin",
            country_name="Japan",
            city_name="Yufuin",
            generated_at="2026-04-20T00:00:00+00:00",
            place_count=1,
            places=[
                NormalizedPlace(
                    id=shared_place_id,
                    name="Share",
                    maps_url="https://maps.google.com/?cid=222",
                    cid="222",
                    main_photo_path="/place-photos/share.webp",
                    user_rating_count=100,
                    status="active",
                )
            ],
        )
        oita_cache_entry = EnrichmentCacheEntry(
            fetched_at="2026-04-20T00:00:00+00:00",
            query="Milch, Oita",
            matched=True,
            place=EnrichmentPlace(
                google_place_id="place-milch",
                google_place_resource_name="places/place-milch",
                display_name="Milch",
            ),
        )
        yufuin_cache_entry = EnrichmentCacheEntry(
            fetched_at="2026-04-21T00:00:00+00:00",
            query="Share, Yufuin",
            matched=True,
            place=EnrichmentPlace(
                display_name="Share",
                photo_url="https://images.example/share.webp",
            ),
        )

        rows = build_data.build_places_sqlite_rows(
            raw_lists={
                "oita-japan": oita_raw,
                "yufuin-japan": yufuin_raw,
            },
            guides=[oita_guide, yufuin_guide],
            enrichment_caches={
                "oita-japan": {shared_place_id: oita_cache_entry},
                "yufuin-japan": {shared_place_id: yufuin_cache_entry},
            },
        )

        self.assertEqual(len(rows.canonical_place_rows), 1)
        canonical_row = rows.canonical_place_rows[0]
        self.assertEqual(canonical_row[1], "oita-japan")
        self.assertEqual(canonical_row[10], "Milch")
        self.assertEqual(canonical_row[43], "place-milch")
        self.assertIsNone(canonical_row[59])

    def test_rebuild_places_sqlite_skips_rewrite_when_signature_matches(self) -> None:
        raw = RawSavedList(
            configured_source_type="google_list_url",
            fetched_at="2026-04-20T00:00:00+00:00",
            title="Tokyo",
            places=[
                RawPlace(
                    name="Coffee House",
                    address="1 Shibuya, Tokyo, Japan",
                    maps_url="https://maps.google.com/?cid=111",
                    cid="111",
                    lat=35.6595,
                    lng=139.7005,
                )
            ],
        )
        place_id = build_data.stable_place_id(raw.places[0], source_type=raw.configured_source_type)
        cache_entry = EnrichmentCacheEntry(
            fetched_at="2026-04-20T00:00:00+00:00",
            last_verified_at="2026-04-20T00:00:00+00:00",
            refresh_after="2026-04-27T00:00:00+00:00",
            source="google_maps_page",
            query="Coffee House, 1 Shibuya, Tokyo, Japan",
            input_signature=build_data.place_input_signature(raw.places[0]),
            matched=True,
            score=build_data.STRONG_MATCH_SCORE,
            place=EnrichmentPlace(display_name="Coffee House"),
        )
        guide = Guide(
            slug="tokyo-japan",
            title="Tokyo",
            country_name="Japan",
            city_name="Tokyo",
            generated_at="2026-04-20T00:00:00+00:00",
            place_count=1,
            featured_place_ids=[place_id],
            best_hit_place_ids=[place_id],
            top_categories=["cafe"],
            places=[
                NormalizedPlace(
                    id=place_id,
                    name="Coffee House",
                    address="1 Shibuya, Tokyo, Japan",
                    maps_url="https://www.google.com/maps/search/?api=1&query=Coffee+House",
                    status="active",
                )
            ],
        )

        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            db_path = tmpdir_path / "cache" / "places.sqlite"
            db_path.parent.mkdir(parents=True, exist_ok=True)

            with patch.object(build_data, "PLACES_SQLITE_PATH", db_path):
                build_data.rebuild_places_sqlite(
                    raw_lists={"tokyo-japan": raw},
                    guides=[guide],
                    enrichment_caches={"tokyo-japan": {place_id: cache_entry}},
                )
                first_stat = db_path.stat()
                build_data.rebuild_places_sqlite(
                    raw_lists={"tokyo-japan": raw},
                    guides=[guide],
                    enrichment_caches={"tokyo-japan": {place_id: cache_entry}},
                )
                second_stat = db_path.stat()

            self.assertEqual(first_stat.st_mtime_ns, second_stat.st_mtime_ns)

    def test_rebuild_places_sqlite_recovers_from_invalid_existing_db(self) -> None:
        raw = RawSavedList(
            configured_source_type="google_list_url",
            fetched_at="2026-04-20T00:00:00+00:00",
            title="Tokyo",
            places=[
                RawPlace(
                    name="Coffee House",
                    address="1 Shibuya, Tokyo, Japan",
                    maps_url="https://maps.google.com/?cid=111",
                    cid="111",
                )
            ],
        )
        place_id = build_data.stable_place_id(raw.places[0], source_type=raw.configured_source_type)
        guide = Guide(
            slug="tokyo-japan",
            title="Tokyo",
            country_name="Japan",
            city_name="Tokyo",
            generated_at="2026-04-20T00:00:00+00:00",
            place_count=1,
            places=[
                NormalizedPlace(
                    id=place_id,
                    name="Coffee House",
                    maps_url="https://www.google.com/maps/search/?api=1&query=Coffee+House",
                    status="active",
                )
            ],
        )

        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            db_path = tmpdir_path / "cache" / "places.sqlite"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            db_path.write_text("not a sqlite file", encoding="utf-8")

            with patch.object(build_data, "PLACES_SQLITE_PATH", db_path):
                self.assertIsNone(build_data.load_places_cache_from_sqlite("tokyo-japan"))
                self.assertIsNone(build_data.load_places_sqlite_build_signature())

                build_data.rebuild_places_sqlite(
                    raw_lists={"tokyo-japan": raw},
                    guides=[guide],
                    enrichment_caches={"tokyo-japan": {}},
                )

            connection = sqlite3.connect(db_path)
            try:
                build_metadata = connection.execute(
                    "SELECT value FROM build_metadata WHERE key = ?",
                    (build_data.PLACES_SQLITE_BUILD_METADATA_KEY,),
                ).fetchone()
                self.assertIsNotNone(build_metadata)
            finally:
                connection.close()

    def test_load_places_cache_from_sqlite_raises_operational_error(self) -> None:
        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            db_path = tmpdir_path / "cache" / "places.sqlite"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            db_path.touch()

            with (
                patch.object(build_data, "PLACES_SQLITE_PATH", db_path),
                patch.object(build_data, "sqlite_table_exists", side_effect=sqlite3.OperationalError("database is locked")),
            ):
                with self.assertRaises(sqlite3.OperationalError):
                    build_data.load_places_cache_from_sqlite("tokyo-japan")

    def test_load_places_sqlite_build_signature_raises_operational_error(self) -> None:
        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            db_path = tmpdir_path / "cache" / "places.sqlite"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            db_path.touch()

            with (
                patch.object(build_data, "PLACES_SQLITE_PATH", db_path),
                patch.object(build_data, "sqlite_table_exists", side_effect=sqlite3.OperationalError("database is locked")),
            ):
                with self.assertRaises(sqlite3.OperationalError):
                    build_data.load_places_sqlite_build_signature()

    def test_save_places_cache_writes_sqlite_only_by_default(self) -> None:
        cache_entry = EnrichmentCacheEntry(
            fetched_at="2026-04-20T00:00:00+00:00",
            refresh_after="2026-04-27T00:00:00+00:00",
            query="Coffee House, 1 Shibuya, Tokyo, Japan",
            input_signature="signature-v1",
            matched=True,
            score=build_data.STRONG_MATCH_SCORE,
            place=EnrichmentPlace(
                display_name="Coffee House",
                formatted_address="1 Shibuya, Tokyo, Japan",
            ),
        )

        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            cache_dir = tmpdir_path / "cache" / "google-places"
            db_path = tmpdir_path / "cache" / "places.sqlite"

            with (
                patch.object(build_data, "PLACES_CACHE_DIR", cache_dir),
                patch.object(build_data, "PLACES_SQLITE_PATH", db_path),
            ):
                build_data.save_places_cache("tokyo-japan", {"cid:111": cache_entry})

                self.assertFalse((cache_dir / "tokyo-japan.json").exists())

                build_data.write_json(
                    cache_dir / "tokyo-japan.json",
                    {
                        "cid:111": {
                            "fetched_at": "2020-01-01T00:00:00+00:00",
                            "query": "stale json payload",
                            "matched": False,
                        }
                    },
                )

                loaded_payload = build_data.load_places_cache("tokyo-japan")

            self.assertEqual(loaded_payload["cid:111"].query, cache_entry.query)
            self.assertTrue(loaded_payload["cid:111"].matched)

    def test_export_places_cache_json_writes_debug_output(self) -> None:
        cache_entry = EnrichmentCacheEntry(
            fetched_at="2026-04-20T00:00:00+00:00",
            refresh_after="2026-04-27T00:00:00+00:00",
            query="Coffee House, 1 Shibuya, Tokyo, Japan",
            input_signature="signature-v1",
            matched=True,
            score=build_data.STRONG_MATCH_SCORE,
            place=EnrichmentPlace(display_name="Coffee House"),
        )

        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            cache_dir = tmpdir_path / "cache" / "google-places"

            with patch.object(build_data, "PLACES_CACHE_DIR", cache_dir):
                build_data.export_places_cache_json("tokyo-japan", {"cid:111": cache_entry})

            exported = json.loads((cache_dir / "tokyo-japan.json").read_text(encoding="utf-8"))

        self.assertEqual(exported["cid:111"]["query"], cache_entry.query)

    def test_parallel_enrichment_terminates_workers_on_interrupt(self) -> None:
        raw = RawSavedList(
            title="Tokyo",
            places=[
                RawPlace(
                    name="First Place",
                    address="1 Example St, Tokyo",
                    maps_url="https://maps.google.com/?cid=111",
                    cid="111",
                ),
                RawPlace(
                    name="Second Place",
                    address="2 Example St, Tokyo",
                    maps_url="https://maps.google.com/?cid=222",
                    cid="222",
                )
            ],
        )

        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            raw_dir = tmpdir_path / "raw"
            cache_dir = tmpdir_path / "cache"
            raw_dir.mkdir()
            cache_dir.mkdir()
            (raw_dir / "tokyo-japan.json").write_text(
                json.dumps(raw.model_dump(mode="json"), ensure_ascii=False),
                encoding="utf-8",
            )

            class FakeFuture:
                def result(self):
                    raise KeyboardInterrupt()

            future = FakeFuture()
            executor_holder: dict[str, object] = {}

            class FakeExecutor:
                def __init__(self, max_workers):
                    self.max_workers = max_workers
                    self.terminated = False
                    self.shutdown_calls: list[tuple[bool, bool]] = []
                    executor_holder["executor"] = self

                def submit(self, _fn, _slug, _place_id, _place_name, _refresh_reason, place_payload, **kwargs):
                    return future

                def terminate_workers(self):
                    self.terminated = True

                def shutdown(self, wait=True, cancel_futures=False):
                    self.shutdown_calls.append((wait, cancel_futures))

            with (
                patch.object(build_data, "RAW_DIR", raw_dir),
                patch.object(build_data, "PLACES_CACHE_DIR", cache_dir),
                patch.object(build_data, "google_places_api_key", return_value=None),
                patch.object(build_data, "ThreadPoolExecutor", FakeExecutor),
                patch.object(build_data, "as_completed", return_value=[future]),
                patch("builtins.print"),
            ):
                with self.assertRaises(KeyboardInterrupt):
                    build_data.enrich_raw_sources(
                        force_refresh=True,
                        refresh_workers=2,
                        refresh_startup_jitter_seconds=0,
                    )

        executor = executor_holder["executor"]
        assert isinstance(executor, FakeExecutor)
        self.assertTrue(executor.terminated)
        self.assertIn((True, True), executor.shutdown_calls)

    def test_enrich_place_job_logs_when_worker_starts(self) -> None:
        entry = EnrichmentCacheEntry(
            fetched_at="2026-04-20T00:00:00+00:00",
            query="First Place, Tokyo",
            matched=True,
            place=EnrichmentPlace(display_name="First Place"),
        )

        with (
            patch.object(build_data, "sleep_for_refresh_startup_jitter") as jitter_sleep,
            patch.object(build_data, "fetch_places_enrichment", return_value=entry) as fetch_enrichment,
            patch("builtins.print") as print_mock,
        ):
            result = build_data.enrich_place_job(
                "tokyo-japan",
                "cid:111",
                "First Place",
                "forced",
                {
                    "name": "First Place",
                    "address": "1 Example St, Tokyo",
                    "maps_url": "https://maps.google.com/?cid=111",
                    "cid": "111",
                },
                api_key=None,
                refresh_startup_jitter_seconds=3,
            )

        self.assertIs(result, entry)
        print_mock.assert_called_once_with(
            "Enriching tokyo-japan:cid:111 [First Place] (forced)",
            flush=True,
        )
        jitter_sleep.assert_called_once_with(3)
        fetch_enrichment.assert_called_once()

    def test_enrich_place_job_preserves_previous_entry_when_refresh_degrades_to_error(self) -> None:
        previous_entry = EnrichmentCacheEntry(
            fetched_at="2026-04-20T00:00:00+00:00",
            query="First Place, Tokyo",
            matched=True,
            place=EnrichmentPlace(
                display_name="First Place",
                rating=4.7,
                user_rating_count=321,
                primary_type="restaurant",
                primary_type_display_name="Restaurant",
                business_status="CLOSED_TEMPORARILY",
                google_maps_uri="https://www.google.com/maps/search/?api=1&query=First+Place&query_place_id=place123",
                google_place_id="place123",
                photo_url="https://photos.example/old.jpg",
            ),
        )
        degraded_entry = EnrichmentCacheEntry(
            fetched_at="2026-04-23T00:00:00+00:00",
            query="First Place, Tokyo",
            source="google_places_api",
            error="http_403",
        )

        with (
            patch.object(build_data, "sleep_for_refresh_startup_jitter"),
            patch.object(build_data, "fetch_places_enrichment", return_value=degraded_entry),
            patch("builtins.print") as print_mock,
        ):
            result = build_data.enrich_place_job(
                "tokyo-japan",
                "cid:111",
                "First Place",
                "refresh-window-expired",
                {
                    "name": "First Place",
                    "address": "1 Example St, Tokyo",
                    "maps_url": "https://maps.google.com/?cid=111",
                    "cid": "111",
                },
                api_key="test-key",
                refresh_startup_jitter_seconds=0,
                existing_entry=previous_entry,
            )

        self.assertIs(result, previous_entry)
        self.assertEqual(print_mock.call_count, 2)
        self.assertEqual(
            print_mock.call_args_list[1].args[0],
            "WARNING: Preserving previous enrichment for tokyo-japan:cid:111 [First Place] "
            "because refresh returned degraded result (http_403).",
        )

    def test_enrich_place_job_preserves_previous_fields_when_refresh_loses_metadata(self) -> None:
        previous_entry = EnrichmentCacheEntry(
            fetched_at="2026-04-20T00:00:00+00:00",
            query="First Place, Tokyo",
            matched=True,
            place=EnrichmentPlace(
                display_name="First Place",
                rating=4.7,
                user_rating_count=321,
                primary_type="restaurant",
                primary_type_display_name="Restaurant",
                business_status="CLOSED_TEMPORARILY",
                google_maps_uri="https://www.google.com/maps/search/?api=1&query=First+Place&query_place_id=place123",
                google_place_id="place123",
                google_place_resource_name="places/place123",
                photo_url="https://photos.example/old.jpg",
            ),
        )
        refreshed_entry = EnrichmentCacheEntry(
            fetched_at="2026-04-23T00:00:00+00:00",
            query="First Place, Tokyo",
            matched=True,
            place=EnrichmentPlace(
                display_name="First Place",
                google_maps_uri="https://www.google.com/maps/search/?api=1&query=First+Place",
            ),
        )

        with (
            patch.object(build_data, "sleep_for_refresh_startup_jitter"),
            patch.object(build_data, "fetch_places_enrichment", return_value=refreshed_entry),
            patch("builtins.print") as print_mock,
        ):
            result = build_data.enrich_place_job(
                "tokyo-japan",
                "cid:111",
                "First Place",
                "refresh-window-expired",
                {
                    "name": "First Place",
                    "address": "1 Example St, Tokyo",
                    "maps_url": "https://maps.google.com/?cid=111",
                    "cid": "111",
                },
                api_key=None,
                refresh_startup_jitter_seconds=0,
                existing_entry=previous_entry,
            )

        self.assertIs(result, refreshed_entry)
        self.assertEqual(result.place.rating, 4.7)
        self.assertEqual(result.place.user_rating_count, 321)
        self.assertEqual(result.place.primary_type_display_name, "Restaurant")
        self.assertEqual(result.place.business_status, "CLOSED_TEMPORARILY")
        self.assertEqual(
            result.place.google_maps_uri,
            "https://www.google.com/maps/search/?api=1&query=First+Place&query_place_id=place123",
        )
        self.assertEqual(result.place.google_place_id, "place123")
        self.assertEqual(result.place.photo_url, "https://photos.example/old.jpg")
        self.assertEqual(print_mock.call_count, 2)
        self.assertEqual(
            print_mock.call_args_list[1].args[0],
            "WARNING: Preserving previous enrichment fields for tokyo-japan:cid:111 [First Place]: "
            "rating, user_rating_count, primary_category, status, maps_url, photo_url.",
        )

    def test_enrich_place_job_does_not_preserve_suspicious_old_photo_urls(self) -> None:
        previous_entry = EnrichmentCacheEntry(
            fetched_at="2026-04-20T00:00:00+00:00",
            query="First Place, Tokyo",
            matched=True,
            place=EnrichmentPlace(
                display_name="First Place",
                rating=4.7,
                user_rating_count=321,
                google_maps_uri="https://www.google.com/maps/search/?api=1&query=First+Place&query_place_id=place123",
                google_place_id="place123",
                photo_url="https://lh3.googleusercontent.com/a-/ALV-UjW_avatar=w36-h36-p-rp-mo-br100",
            ),
        )
        refreshed_entry = EnrichmentCacheEntry(
            fetched_at="2026-04-23T00:00:00+00:00",
            query="First Place, Tokyo",
            matched=True,
            place=EnrichmentPlace(
                display_name="First Place",
                google_maps_uri="https://www.google.com/maps/search/?api=1&query=First+Place",
            ),
        )

        with (
            patch.object(build_data, "sleep_for_refresh_startup_jitter"),
            patch.object(build_data, "fetch_places_enrichment", return_value=refreshed_entry),
            patch("builtins.print") as print_mock,
        ):
            result = build_data.enrich_place_job(
                "tokyo-japan",
                "cid:111",
                "First Place",
                "refresh-window-expired",
                {
                    "name": "First Place",
                    "address": "1 Example St, Tokyo",
                    "maps_url": "https://maps.google.com/?cid=111",
                    "cid": "111",
                },
                api_key=None,
                refresh_startup_jitter_seconds=0,
                existing_entry=previous_entry,
            )

        self.assertIs(result, refreshed_entry)
        self.assertIsNone(result.place.photo_url)
        self.assertEqual(print_mock.call_count, 2)
        self.assertEqual(
            print_mock.call_args_list[1].args[0],
            "WARNING: Preserving previous enrichment fields for tokyo-japan:cid:111 [First Place]: "
            "rating, user_rating_count, maps_url.",
        )

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
