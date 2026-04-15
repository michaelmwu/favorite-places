from __future__ import annotations

import json
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from scripts import build_data
from scripts.pipeline_models import EnrichmentCacheEntry, EnrichmentPlace, RawPlace, RawSavedList, SourceConfig


class BuildDataTests(unittest.TestCase):
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
                    maps_url="https://maps.google.com/?cid=1",
                    cid="111",
                ),
                RawPlace(
                    name="Noodle Shop",
                    address="2 Shinjuku, Tokyo, Japan",
                    note="Best at lunch",
                    is_favorite=True,
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

        first_place = guide.places[0]
        hidden_place = next(place for place in guide.places if place.id == third_place_id)

        self.assertEqual(first_place.id, first_place_id)
        self.assertEqual(first_place.primary_category, "Bakery")
        self.assertEqual(first_place.note, "Manual note")
        self.assertEqual(first_place.maps_url, "https://maps.google.com/?cid=override")
        self.assertEqual(first_place.neighborhood, "Shibuya")
        self.assertTrue(first_place.top_pick)
        self.assertEqual(first_place.status, "active")
        self.assertIn("bakery", first_place.tags)
        self.assertIn("shibuya", first_place.tags)
        self.assertIn("specialty", first_place.tags)
        self.assertIn("tokyo", first_place.tags)
        self.assertTrue(hidden_place.hidden)

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
                )

        scrape.assert_called_once_with(source, headed=False)


if __name__ == "__main__":
    unittest.main()
