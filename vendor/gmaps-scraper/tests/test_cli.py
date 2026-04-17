from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import Mock, patch

from gmaps_scraper.cli import main
from gmaps_scraper.models import PlaceDetails
from gmaps_scraper.scraper import BrowserArtifacts, BrowserSessionConfig, HttpSessionConfig


def _artifacts() -> BrowserArtifacts:
    return BrowserArtifacts(
        resolved_url=(
            "https://www.google.com/maps/@30.5370705,125.4120472,6z/"
            "data=!4m3!11m2!2sUGEPbA20Qd-OH4uoWjmDgQ!3e3?entry=ttu"
        ),
        runtime_state=["runtime"],
        script_texts=["script"],
        html="<html></html>",
    )


def _parsed_payload() -> dict[str, object]:
    return {
        "source_url": "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
        "resolved_url": (
            "https://www.google.com/maps/@30.5370705,125.4120472,6z/"
            "data=!4m3!11m2!2sUGEPbA20Qd-OH4uoWjmDgQ!3e3?entry=ttu"
        ),
        "list_id": "UGEPbA20Qd-OH4uoWjmDgQ",
        "title": "Tokyo Dinners",
        "description": "Best spots in the city",
        "places": [],
    }


def _result(payload: dict[str, object]) -> Mock:
    result = Mock()
    result.to_dict.return_value = payload
    return result


class CliTests(unittest.TestCase):
    def test_prints_json_to_stdout(self) -> None:
        stdout = io.StringIO()
        artifacts = _artifacts()
        parsed_payload = _parsed_payload()
        result = _result(parsed_payload)

        with (
            patch(
                "sys.argv",
                ["gmaps-scraper", "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18"],
            ),
            patch(
                "gmaps_scraper.cli.collect_saved_list_result",
                return_value=(artifacts, result),
            ) as collect_saved_list_result,
            redirect_stdout(stdout),
        ):
            exit_code = main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), parsed_payload)
        collect_saved_list_result.assert_called_once_with(
            "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
            headless=True,
            timeout_ms=30_000,
            settle_time_ms=3_000,
            collection_mode="auto",
            browser_session=None,
            http_session=None,
        )

    def test_writes_output_file_and_forwards_cli_flags(self) -> None:
        artifacts = _artifacts()
        parsed_payload = _parsed_payload()
        result = _result(parsed_payload)

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "saved-list.json"
            with (
                patch(
                    "sys.argv",
                    [
                        "gmaps-scraper",
                        "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
                        "--output",
                        str(output_path),
                        "--headed",
                        "--timeout-ms",
                        "45000",
                        "--settle-ms",
                        "5000",
                        "--session-dir",
                        str(Path(tmp_dir) / "session"),
                        "--proxy",
                        "http://proxy.example:8080",
                        "--http-cookie-jar",
                        str(Path(tmp_dir) / "cookies.txt"),
                    ],
                ),
                patch(
                    "gmaps_scraper.cli.collect_saved_list_result",
                    return_value=(artifacts, result),
                ) as collect_saved_list_result,
            ):
                exit_code = main()

            self.assertEqual(exit_code, 0)
            self.assertEqual(
                json.loads(output_path.read_text(encoding="utf-8")),
                parsed_payload,
            )
            collect_saved_list_result.assert_called_once_with(
                "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
                headless=False,
                timeout_ms=45_000,
                settle_time_ms=5_000,
                collection_mode="auto",
                browser_session=BrowserSessionConfig(
                    profile_dir=Path(tmp_dir) / "session",
                    proxy="http://proxy.example:8080",
                ),
                http_session=HttpSessionConfig(
                    cookie_jar_path=Path(tmp_dir) / "cookies.txt",
                    proxy="http://proxy.example:8080",
                ),
            )

    def test_forwards_explicit_collection_mode(self) -> None:
        stdout = io.StringIO()
        artifacts = _artifacts()
        parsed_payload = _parsed_payload()
        result = _result(parsed_payload)

        with (
            patch(
                "sys.argv",
                [
                    "gmaps-scraper",
                    "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
                    "--fetch-mode",
                    "browser",
                ],
            ),
            patch(
                "gmaps_scraper.cli.collect_saved_list_result",
                return_value=(artifacts, result),
            ) as collect_saved_list_result,
            redirect_stdout(stdout),
        ):
            exit_code = main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), parsed_payload)
        collect_saved_list_result.assert_called_once_with(
            "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
            headless=True,
            timeout_ms=30_000,
            settle_time_ms=3_000,
            collection_mode="browser",
            browser_session=None,
            http_session=None,
        )

    def test_place_kind_calls_place_scraper(self) -> None:
        stdout = io.StringIO()
        details = PlaceDetails(
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

        with (
            patch(
                "sys.argv",
                [
                    "gmaps-scraper",
                    "https://www.google.com/maps/place/Den",
                    "--kind",
                    "place",
                ],
            ),
            patch("gmaps_scraper.cli.scrape_place", return_value=details) as scrape_place,
            patch("gmaps_scraper.cli.collect_saved_list_result") as collect_saved_list_result,
            redirect_stdout(stdout),
        ):
            exit_code = main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), details.to_dict())
        scrape_place.assert_called_once_with(
            "https://www.google.com/maps/place/Den",
            headless=True,
            timeout_ms=30_000,
            settle_time_ms=3_000,
            browser_session=None,
            http_session=None,
        )
        collect_saved_list_result.assert_not_called()

    def test_uses_proxy_from_environment_for_list_scrapes(self) -> None:
        stdout = io.StringIO()
        artifacts = _artifacts()
        parsed_payload = _parsed_payload()
        result = _result(parsed_payload)

        with (
            patch(
                "sys.argv",
                ["gmaps-scraper", "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18"],
            ),
            patch.dict("os.environ", {"GMAPS_SCRAPER_PROXY": "http://proxy.example:8080"}),
            patch(
                "gmaps_scraper.cli.collect_saved_list_result",
                return_value=(artifacts, result),
            ) as collect_saved_list_result,
            redirect_stdout(stdout),
        ):
            exit_code = main()

        self.assertEqual(exit_code, 0)
        collect_saved_list_result.assert_called_once_with(
            "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
            headless=True,
            timeout_ms=30_000,
            settle_time_ms=3_000,
            collection_mode="auto",
            browser_session=BrowserSessionConfig(
                profile_dir=None,
                proxy="http://proxy.example:8080",
            ),
            http_session=HttpSessionConfig(
                cookie_jar_path=None,
                proxy="http://proxy.example:8080",
            ),
        )

    def test_place_kind_forwards_http_cookie_jar_to_preview_enrichment(self) -> None:
        stdout = io.StringIO()
        details = PlaceDetails(
            source_url="https://www.google.com/maps/place/Den",
            resolved_url="https://www.google.com/maps/place/Den/@35.6731762,139.7127216,17z",
            name="Den",
            category="Japanese restaurant",
            rating=4.4,
            review_count=324,
            address="Japan, 〒150-0001 Tokyo, Shibuya, Jingumae, 2 Chome−3−18",
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            with (
                patch(
                    "sys.argv",
                    [
                        "gmaps-scraper",
                        "https://www.google.com/maps/place/Den",
                        "--kind",
                        "place",
                        "--http-cookie-jar",
                        str(Path(tmp_dir) / "cookies.txt"),
                    ],
                ),
                patch("gmaps_scraper.cli.scrape_place", return_value=details) as scrape_place,
                redirect_stdout(stdout),
            ):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        scrape_place.assert_called_once_with(
            "https://www.google.com/maps/place/Den",
            headless=True,
            timeout_ms=30_000,
            settle_time_ms=3_000,
            browser_session=None,
            http_session=HttpSessionConfig(
                cookie_jar_path=Path(tmp_dir) / "cookies.txt",
                proxy=None,
            ),
        )

    def test_debug_output_dir_writes_dump_and_stdout_payload(self) -> None:
        artifacts = _artifacts()
        parsed_payload = _parsed_payload()
        result = _result(parsed_payload)

        with tempfile.TemporaryDirectory() as tmp_dir:
            stdout = io.StringIO()
            with (
                patch(
                    "gmaps_scraper.cli.collect_saved_list_result",
                    return_value=(artifacts, result),
                ),
                patch("gmaps_scraper.cli.write_debug_dump") as write_debug_dump,
                patch(
                    "sys.argv",
                    [
                        "gmaps-scraper",
                        "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
                        "--debug-output-dir",
                        tmp_dir,
                    ],
                ),
                redirect_stdout(stdout),
            ):
                exit_code = main()

            self.assertEqual(exit_code, 0)
            write_debug_dump.assert_called_once_with(
                "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
                resolved_url=artifacts.resolved_url,
                runtime_state=artifacts.runtime_state,
                script_texts=artifacts.script_texts,
                html=artifacts.html,
                output_dir=Path(tmp_dir),
            )
            self.assertEqual(json.loads(stdout.getvalue()), parsed_payload)

    def test_dump_debug_output_uses_default_hidden_directory(self) -> None:
        artifacts = _artifacts()
        parsed_payload = _parsed_payload()
        result = _result(parsed_payload)

        with tempfile.TemporaryDirectory() as tmp_dir:
            stdout = io.StringIO()
            with (
                patch(
                    "gmaps_scraper.cli.collect_saved_list_result",
                    return_value=(artifacts, result),
                ),
                patch("gmaps_scraper.cli.write_debug_dump") as write_debug_dump,
                patch("gmaps_scraper.cli.os.getcwd", return_value=tmp_dir),
                patch(
                    "sys.argv",
                    [
                        "gmaps-scraper",
                        "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
                        "--dump-debug-output",
                    ],
                ),
                redirect_stdout(stdout),
            ):
                exit_code = main()

            self.assertEqual(exit_code, 0)
            write_debug_dump.assert_called_once_with(
                "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
                resolved_url=artifacts.resolved_url,
                runtime_state=artifacts.runtime_state,
                script_texts=artifacts.script_texts,
                html=artifacts.html,
                output_dir=Path(tmp_dir) / ".gmaps-debug" / "UGEPbA20Qd-OH4uoWjmDgQ",
            )
            self.assertEqual(json.loads(stdout.getvalue()), parsed_payload)

    def test_debug_output_dir_overrides_default_dump_directory(self) -> None:
        artifacts = _artifacts()
        parsed_payload = _parsed_payload()
        result = _result(parsed_payload)

        with tempfile.TemporaryDirectory() as tmp_dir:
            stdout = io.StringIO()
            explicit_dir = Path(tmp_dir) / "custom-debug"
            with (
                patch(
                    "gmaps_scraper.cli.collect_saved_list_result",
                    return_value=(artifacts, result),
                ),
                patch("gmaps_scraper.cli.write_debug_dump") as write_debug_dump,
                patch("gmaps_scraper.cli.os.getcwd", return_value=tmp_dir),
                patch(
                    "sys.argv",
                    [
                        "gmaps-scraper",
                        "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
                        "--dump-debug-output",
                        "--debug-output-dir",
                        str(explicit_dir),
                    ],
                ),
                redirect_stdout(stdout),
            ):
                exit_code = main()

            self.assertEqual(exit_code, 0)
            write_debug_dump.assert_called_once_with(
                "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
                resolved_url=artifacts.resolved_url,
                runtime_state=artifacts.runtime_state,
                script_texts=artifacts.script_texts,
                html=artifacts.html,
                output_dir=explicit_dir,
            )
            self.assertEqual(json.loads(stdout.getvalue()), parsed_payload)

    def test_list_kind_is_accepted(self) -> None:
        stdout = io.StringIO()
        artifacts = _artifacts()
        parsed_payload = _parsed_payload()
        result = _result(parsed_payload)

        with (
            patch(
                "sys.argv",
                [
                    "gmaps-scraper",
                    "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
                    "--kind",
                    "list",
                ],
            ),
            patch(
                "gmaps_scraper.cli.collect_saved_list_result",
                return_value=(artifacts, result),
            ) as collect_saved_list_result,
            redirect_stdout(stdout),
        ):
            exit_code = main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), parsed_payload)
        collect_saved_list_result.assert_called_once()


if __name__ == "__main__":
    unittest.main()
