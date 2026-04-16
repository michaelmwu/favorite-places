from __future__ import annotations

import unittest

from gmaps_scraper import (
    BrowserProxyConfig,
    BrowserSessionConfig,
    HttpSessionConfig,
    ParseError,
    Place,
    PlaceDetails,
    SavedList,
    ScrapeError,
    scrape_place,
    scrape_saved_list,
)


class PublicApiTests(unittest.TestCase):
    def test_top_level_exports_are_importable(self) -> None:
        self.assertTrue(callable(scrape_saved_list))
        self.assertTrue(callable(scrape_place))
        self.assertEqual(BrowserSessionConfig.__name__, "BrowserSessionConfig")
        self.assertEqual(BrowserProxyConfig.__name__, "BrowserProxyConfig")
        self.assertEqual(HttpSessionConfig.__name__, "HttpSessionConfig")
        self.assertTrue(issubclass(ParseError, RuntimeError))
        self.assertTrue(issubclass(ScrapeError, RuntimeError))

    def test_saved_list_serializes_library_shape(self) -> None:
        place = Place(
            name="Yakumo",
            address="Shibuya, Tokyo",
            note="Delicious wonton ramen. You can ask for a mix of white and dark broth.",
            lat=35.6501307,
            lng=139.6868459,
            maps_url="https://maps.google.com/?cid=7451636382641713350",
            is_favorite=True,
        )
        saved_list = SavedList(
            source_url="https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
            resolved_url=(
                "https://www.google.com/maps/@30.5370705,125.4120472,6z/"
                "data=!4m3!11m2!2sUGEPbA20Qd-OH4uoWjmDgQ!3e3?entry=ttu"
            ),
            list_id="UGEPbA20Qd-OH4uoWjmDgQ",
            title="Tokyo Dinners",
            description="Best spots in the city",
            places=[place],
        )

        self.assertEqual(
            saved_list.to_dict(),
            {
                "source_url": "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
                "resolved_url": (
                    "https://www.google.com/maps/@30.5370705,125.4120472,6z/"
                    "data=!4m3!11m2!2sUGEPbA20Qd-OH4uoWjmDgQ!3e3?entry=ttu"
                ),
                "list_id": "UGEPbA20Qd-OH4uoWjmDgQ",
                "title": "Tokyo Dinners",
                "description": "Best spots in the city",
                "places": [
                    {
                        "name": "Yakumo",
                        "address": "Shibuya, Tokyo",
                        "note": (
                            "Delicious wonton ramen. You can ask for a mix of white and "
                            "dark broth."
                        ),
                        "is_favorite": True,
                        "lat": 35.6501307,
                        "lng": 139.6868459,
                        "maps_url": "https://maps.google.com/?cid=7451636382641713350",
                    }
                ],
            },
        )

    def test_place_details_omit_missing_fields(self) -> None:
        place = PlaceDetails(
            source_url="https://www.google.com/maps/place/Den",
            resolved_url="https://www.google.com/maps/place/Den/@35.6731762,139.7127216,17z",
            name="Den",
            secondary_name="傳",
            category="Japanese restaurant",
            rating=4.4,
            review_count=324,
            address="Japan, 〒150-0001 Tokyo, Shibuya, Jingumae, 2 Chome−3−18 建築家会館ＪＩＡ館",
            status="Closed · Opens 6 PM",
            website="http://www.jimbochoden.com/",
            phone="+81 3-6455-5433",
            plus_code="MPF7+73 Shibuya, Tokyo, Japan",
            lat=35.6731762,
            lng=139.7127216,
            limited_view=True,
        )

        self.assertEqual(
            place.to_dict(),
            {
                "source_url": "https://www.google.com/maps/place/Den",
                "resolved_url": (
                    "https://www.google.com/maps/place/Den/@35.6731762,139.7127216,17z"
                ),
                "name": "Den",
                "category": "Japanese restaurant",
                "rating": 4.4,
                "review_count": 324,
                "address": (
                    "Japan, 〒150-0001 Tokyo, Shibuya, Jingumae, 2 Chome−3−18 "
                    "建築家会館ＪＩＡ館"
                ),
                "status": "Closed · Opens 6 PM",
                "website": "http://www.jimbochoden.com/",
                "phone": "+81 3-6455-5433",
                "plus_code": "MPF7+73 Shibuya, Tokyo, Japan",
                "secondary_name": "傳",
                "lat": 35.6731762,
                "lng": 139.7127216,
                "limited_view": True,
            },
        )


if __name__ == "__main__":
    unittest.main()
