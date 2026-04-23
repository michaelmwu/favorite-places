from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.refresh_summary import build_summary


class RefreshSummaryTests(unittest.TestCase):
    def test_build_summary_parses_github_actions_log_format(self) -> None:
        log_text = "\n".join(
            [
                "refresh\tRun self-hosted refresh\t2026-04-23T06:20:24.0000000Z Refreshing tokyo-japan from https://maps.example (refresh-window-expired)",
                "refresh\tRun self-hosted refresh\t2026-04-23T06:20:25.0000000Z Importing taipei-taiwan from data/imports/taipei.csv",
                "refresh\tRun self-hosted refresh\t2026-04-23T06:20:26.0000000Z Enriching tokyo-japan:cid:1 [Coffee Shop] (missing-cache-entry)",
                "refresh\tRun self-hosted refresh\t2026-04-23T06:20:27.0000000Z Enriching tokyo-japan:cid:2 [Bar] (refresh-window-expired)",
                "refresh\tRun self-hosted refresh\t2026-04-23T06:20:28.0000000Z Downloading 2 place photos with 1 workers (5 existing, 3 without photo URLs)",
                "refresh\tRun self-hosted refresh\t2026-04-23T06:20:29.0000000Z [photos 1/2] downloaded: tokyo-japan / Coffee Shop",
                "refresh\tRun self-hosted refresh\t2026-04-23T06:20:30.0000000Z [photos 2/2] failed: tokyo-japan / Bar",
                "refresh\tRun self-hosted refresh\t2026-04-23T06:20:31.0000000Z WARNING: Preserving previous enrichment for tokyo-japan:cid:2 [Bar] because refresh returned degraded result (http_403).",
                "refresh\tRun self-hosted refresh\t2026-04-23T06:20:32.0000000Z WARNING: Reusing existing local photo for tokyo-japan:cid:3 because current enrichment did not yield a photo URL. Photo extraction may be failing on this runner.",
            ]
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            log_path = Path(tmp_dir) / "refresh.log"
            log_path.write_text(log_text, encoding="utf-8")

            summary = build_summary(log_path)

        self.assertIn("### Raw sources", summary)
        self.assertIn("- Refreshed from remote sources: 1", summary)
        self.assertIn("- Imported CSV sources: 1", summary)
        self.assertIn("- TTL expiry: 1", summary)

        self.assertIn("### Places", summary)
        self.assertIn("- New places backfilled: 1", summary)
        self.assertIn("- Existing places refreshed: 1", summary)
        self.assertIn("- Places refreshed after TTL expiry: 1", summary)

        self.assertIn("### Photos", summary)
        self.assertIn("- Queued for download: 2", summary)
        self.assertIn("- Downloaded: 1", summary)
        self.assertIn("- Reused existing: 5", summary)
        self.assertIn("- Missing photo URLs: 3", summary)
        self.assertIn("- Failed downloads: 1", summary)

        self.assertIn("### Warnings", summary)
        self.assertIn("- Preserved previous enrichment: 1", summary)
        self.assertIn("- Photo extraction warnings: 1", summary)

    def test_build_summary_keeps_non_timestamp_z_content(self) -> None:
        log_text = "\n".join(
            [
                "Enriching tokyo-japan:cid:1 [Cafe Z House] (missing-cache-entry)",
                "[photos 1/1] downloaded: tokyo-japan / Cafe Z House",
            ]
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            log_path = Path(tmp_dir) / "refresh.log"
            log_path.write_text(log_text, encoding="utf-8")

            summary = build_summary(log_path)

        self.assertIn("### Places", summary)
        self.assertIn("- New places backfilled: 1", summary)
        self.assertIn("### Photos", summary)
        self.assertIn("- Downloaded: 1", summary)


if __name__ == "__main__":
    unittest.main()
