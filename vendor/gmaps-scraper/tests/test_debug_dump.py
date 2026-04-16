from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from gmaps_scraper.debug_dump import write_debug_dump

_LIST_URL = (
    "https://www.google.com/maps/@35.6501307,139.6868459,15z/"
    "data=!4m3!11m2!2sUGEPbA20Qd-OH4uoWjmDgQ!3e3"
)
_LIST_NODE = [
    ["UGEPbA20Qd-OH4uoWjmDgQ", 1, None, 1, 1],
    4,
    "https://www.google.com/maps/placelists/list/UGEPbA20Qd-OH4uoWjmDgQ",
    "Owner",
    "Tokyo Dinners",
    "Best spots in the city",
    None,
    None,
    [
        [
            None,
            [
                None,
                None,
                "",
                None,
                "Shibuya, Tokyo",
                [None, None, 35.6501307, 139.6868459],
                ["7451636382641713350", "aux"],
                "/g/11yakumo",
                "Reservation note: order the udon",
            ],
            "Yakumo",
        ]
    ],
]


class DebugDumpTests(unittest.TestCase):
    def test_writes_summary_candidates_and_place_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            summary_path = write_debug_dump(
                _LIST_URL,
                runtime_state=["noise", _LIST_NODE],
                script_texts=[],
                html="<html></html>",
                output_dir=Path(tmp_dir),
            )

            summary = json.loads(summary_path.read_text(encoding="utf-8"))

            self.assertEqual(summary["list_id"], "UGEPbA20Qd-OH4uoWjmDgQ")
            self.assertGreaterEqual(summary["candidate_count"], 1)
            self.assertEqual(len(summary["places"]), 1)

            candidate_path = Path(tmp_dir) / summary["candidates"][0]["file"]
            place_summary_path = Path(tmp_dir) / summary["places"][0]["summary_file"]

            self.assertTrue(candidate_path.exists())
            self.assertTrue(place_summary_path.exists())

            place_summary = json.loads(place_summary_path.read_text(encoding="utf-8"))
            self.assertEqual(place_summary["name"], "Yakumo")
            self.assertIn("Reservation note: order the udon", place_summary["strings"])
