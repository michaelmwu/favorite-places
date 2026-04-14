from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from google_saved_lists.cli import main
from google_saved_lists.scraper import BrowserArtifacts


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


class CliTests(unittest.TestCase):
    def test_prints_json_to_stdout(self) -> None:
        stdout = io.StringIO()
        artifacts = _artifacts()
        parsed_payload = _parsed_payload()

        with (
            patch(
                "sys.argv",
                ["google-saved-lists", "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18"],
            ),
            patch(
                "google_saved_lists.cli.collect_browser_artifacts",
                return_value=artifacts,
            ) as collect_browser_artifacts,
            patch("google_saved_lists.cli.parse_saved_list_artifacts") as parse_saved_list,
            redirect_stdout(stdout),
        ):
            parse_saved_list.return_value.to_dict.return_value = parsed_payload

            exit_code = main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), parsed_payload)
        collect_browser_artifacts.assert_called_once_with(
            "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
            headless=True,
            timeout_ms=30_000,
            settle_time_ms=3_000,
        )
        parse_saved_list.assert_called_once_with(
            "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
            resolved_url=artifacts.resolved_url,
            runtime_state=artifacts.runtime_state,
            script_texts=artifacts.script_texts,
            html=artifacts.html,
        )

    def test_writes_output_file_and_forwards_cli_flags(self) -> None:
        artifacts = _artifacts()
        parsed_payload = _parsed_payload()

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "saved-list.json"
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
                    "google_saved_lists.cli.collect_browser_artifacts",
                    return_value=artifacts,
                ) as collect_browser_artifacts,
                patch("google_saved_lists.cli.parse_saved_list_artifacts") as parse_saved_list,
            ):
                parse_saved_list.return_value.to_dict.return_value = parsed_payload

                exit_code = main()

            self.assertEqual(exit_code, 0)
            self.assertEqual(
                json.loads(output_path.read_text(encoding="utf-8")),
                parsed_payload,
            )
            collect_browser_artifacts.assert_called_once_with(
                "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
                headless=False,
                timeout_ms=45_000,
                settle_time_ms=5_000,
            )
            parse_saved_list.assert_called_once_with(
                "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
                resolved_url=artifacts.resolved_url,
                runtime_state=artifacts.runtime_state,
                script_texts=artifacts.script_texts,
                html=artifacts.html,
            )

    def test_debug_output_dir_writes_dump_and_stdout_payload(self) -> None:
        artifacts = _artifacts()
        parsed_payload = _parsed_payload()

        with tempfile.TemporaryDirectory() as tmp_dir:
            stdout = io.StringIO()
            with (
                patch("google_saved_lists.cli.collect_browser_artifacts", return_value=artifacts),
                patch("google_saved_lists.cli.write_debug_dump") as write_debug_dump,
                patch("google_saved_lists.cli.parse_saved_list_artifacts") as parse_saved_list,
                patch(
                    "sys.argv",
                    [
                        "google-saved-lists",
                        "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
                        "--debug-output-dir",
                        tmp_dir,
                    ],
                ),
                redirect_stdout(stdout),
            ):
                parse_saved_list.return_value.to_dict.return_value = parsed_payload

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

        with tempfile.TemporaryDirectory() as tmp_dir:
            stdout = io.StringIO()
            with (
                patch("google_saved_lists.cli.collect_browser_artifacts", return_value=artifacts),
                patch("google_saved_lists.cli.write_debug_dump") as write_debug_dump,
                patch("google_saved_lists.cli.parse_saved_list_artifacts") as parse_saved_list,
                patch("google_saved_lists.cli.os.getcwd", return_value=tmp_dir),
                patch(
                    "sys.argv",
                    [
                        "google-saved-lists",
                        "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
                        "--dump-debug-output",
                    ],
                ),
                redirect_stdout(stdout),
            ):
                parse_saved_list.return_value.to_dict.return_value = parsed_payload

                exit_code = main()

            self.assertEqual(exit_code, 0)
            write_debug_dump.assert_called_once_with(
                "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
                resolved_url=artifacts.resolved_url,
                runtime_state=artifacts.runtime_state,
                script_texts=artifacts.script_texts,
                html=artifacts.html,
                output_dir=Path(tmp_dir) / ".google-saved-lists-debug" / "UGEPbA20Qd-OH4uoWjmDgQ",
            )
            self.assertEqual(json.loads(stdout.getvalue()), parsed_payload)

    def test_debug_output_dir_overrides_default_dump_directory(self) -> None:
        artifacts = _artifacts()
        parsed_payload = _parsed_payload()

        with tempfile.TemporaryDirectory() as tmp_dir:
            stdout = io.StringIO()
            explicit_dir = Path(tmp_dir) / "custom-debug"
            with (
                patch("google_saved_lists.cli.collect_browser_artifacts", return_value=artifacts),
                patch("google_saved_lists.cli.write_debug_dump") as write_debug_dump,
                patch("google_saved_lists.cli.parse_saved_list_artifacts") as parse_saved_list,
                patch("google_saved_lists.cli.os.getcwd", return_value=tmp_dir),
                patch(
                    "sys.argv",
                    [
                        "google-saved-lists",
                        "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
                        "--dump-debug-output",
                        "--debug-output-dir",
                        str(explicit_dir),
                    ],
                ),
                redirect_stdout(stdout),
            ):
                parse_saved_list.return_value.to_dict.return_value = parsed_payload

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


if __name__ == "__main__":
    unittest.main()
