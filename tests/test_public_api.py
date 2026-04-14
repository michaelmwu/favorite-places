from __future__ import annotations

import unittest

from google_saved_lists import ParseError, Place, SavedList, ScrapeError, scrape_saved_list


class PublicApiTests(unittest.TestCase):
    def test_top_level_exports_are_importable(self) -> None:
        self.assertTrue(callable(scrape_saved_list))
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


if __name__ == "__main__":
    unittest.main()
