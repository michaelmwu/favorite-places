from __future__ import annotations

import unittest

from gmaps_scraper.url_tools import (
    extract_list_id,
    extract_list_id_from_text,
    has_placelist_marker,
)


class UrlToolsTests(unittest.TestCase):
    def test_extract_list_id_from_maps_data_url(self) -> None:
        url = (
            "https://www.google.com/maps/@30.5370705,125.4120472,6z/"
            "data=!4m3!11m2!2sUGEPbA20Qd-OH4uoWjmDgQ!3e3?entry=ttu"
        )

        self.assertEqual(extract_list_id(url), "UGEPbA20Qd-OH4uoWjmDgQ")

    def test_extract_list_id_returns_none_when_absent(self) -> None:
        self.assertEqual(extract_list_id("https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18"), None)

    def test_extract_list_id_from_text_falls_back_to_placelist_marker(self) -> None:
        text = "https://www.google.com/maps/placelists/list/UGEPbA20Qd-OH4uoWjmDgQ"

        self.assertEqual(extract_list_id_from_text(text), "UGEPbA20Qd-OH4uoWjmDgQ")

    def test_detects_placelist_marker(self) -> None:
        self.assertTrue(has_placelist_marker("prefix maps/placelists/list/UGEPbA20Qd-OH4uoWjmDgQ"))
        self.assertFalse(has_placelist_marker("https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18"))


if __name__ == "__main__":
    unittest.main()
