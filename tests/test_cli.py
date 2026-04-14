from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from google_saved_lists.cli import main
from google_saved_lists.models import Place, SavedList


def _saved_list() -> SavedList:
    return SavedList(
        source_url="https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
        resolved_url=(
            "https://www.google.com/maps/@30.5370705,125.4120472,6z/"
            "data=!4m3!11m2!2sUGEPbA20Qd-OH4uoWjmDgQ!3e3?entry=ttu"
        ),
        list_id="UGEPbA20Qd-OH4uoWjmDgQ",
        title="Tokyo Dinners",
        description="Best spots in the city",
        places=[
            Place(
                name="Yakumo",
                address="Shibuya, Tokyo",
                note="Delicious wonton ramen. You can ask for a mix of white and dark broth.",
                lat=35.6501307,
                lng=139.6868459,
                maps_url="https://maps.google.com/?cid=7451636382641713350",
                cid="7451636382641713350",
                is_favorite=True,
            )
        ],
    )


class CliTests(unittest.TestCase):
    def test_prints_json_to_stdout(self) -> None:
        stdout = io.StringIO()
        with (
            patch("sys.argv", ["google-saved-lists", "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18"]),
            patch("google_saved_lists.cli.scrape_saved_list", return_value=_saved_list()) as scrape,
            redirect_stdout(stdout),
        ):
            exit_code = main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            json.loads(stdout.getvalue()),
            _saved_list().to_dict(),
        )
        scrape.assert_called_once_with(
            "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
            headless=True,
            timeout_ms=30_000,
            settle_time_ms=3_000,
        )

    def test_writes_output_file_and_forwards_cli_flags(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "saved-list.json"

            with (
                patch(
                    "sys.argv",
                    [
                        "google-saved-lists",
                        "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
                        "--output",
                        str(output_path),
                        "--headed",
                        "--timeout-ms",
                        "45000",
                        "--settle-ms",
                        "5000",
                    ],
                ),
                patch(
                    "google_saved_lists.cli.scrape_saved_list",
                    return_value=_saved_list(),
                ) as scrape,
            ):
                exit_code = main()

            self.assertEqual(exit_code, 0)
            self.assertEqual(
                json.loads(output_path.read_text(encoding="utf-8")),
                _saved_list().to_dict(),
            )
            scrape.assert_called_once_with(
                "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
                headless=False,
                timeout_ms=45_000,
                settle_time_ms=5_000,
            )


if __name__ == "__main__":
    unittest.main()
