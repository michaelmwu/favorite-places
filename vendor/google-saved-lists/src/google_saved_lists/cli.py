"""Command-line interface for the Google saved-list scraper."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from google_saved_lists.debug_dump import write_debug_dump
from google_saved_lists.parser import parse_saved_list_artifacts
from google_saved_lists.scraper import collect_browser_artifacts
from google_saved_lists.url_tools import extract_list_id

_DEFAULT_DEBUG_DIR_NAME = ".google-saved-lists-debug"


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", help="Google Maps saved-list URL")
    parser.add_argument("--output", type=Path, help="Optional JSON output path")
    parser.add_argument(
        "--show-browser-window",
        "--headed",
        dest="show_browser_window",
        action="store_true",
        help="Show the browser window while scraping for debugging.",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=30_000,
        help="Navigation timeout in milliseconds.",
    )
    parser.add_argument(
        "--settle-ms",
        type=int,
        default=3_000,
        help="Extra wait time after the page loads.",
    )
    parser.add_argument(
        "--debug-output-dir",
        type=Path,
        help=(
            "Directory for raw runtime artifacts, ranked candidate payloads, "
            "and per-place debug dumps."
        ),
    )
    parser.add_argument(
        "--dump-debug-output",
        action="store_true",
        help=(
            "Write debug artifacts to a default hidden directory in the current "
            f"working directory: `{_DEFAULT_DEBUG_DIR_NAME}`."
        ),
    )
    return parser


def main() -> int:
    """Run the CLI."""
    parser = build_parser()
    args = parser.parse_args()

    artifacts = collect_browser_artifacts(
        args.url,
        headless=not args.show_browser_window,
        timeout_ms=args.timeout_ms,
        settle_time_ms=args.settle_ms,
    )
    debug_output_dir = _resolve_debug_output_dir(
        list_url=args.url,
        resolved_url=artifacts.resolved_url,
        dump_debug_output=args.dump_debug_output,
        debug_output_dir=args.debug_output_dir,
    )
    if debug_output_dir is not None:
        write_debug_dump(
            args.url,
            resolved_url=artifacts.resolved_url,
            runtime_state=artifacts.runtime_state,
            script_texts=artifacts.script_texts,
            html=artifacts.html,
            output_dir=debug_output_dir,
        )
    result = parse_saved_list_artifacts(
        args.url,
        resolved_url=artifacts.resolved_url,
        runtime_state=artifacts.runtime_state,
        script_texts=artifacts.script_texts,
        html=artifacts.html,
    )
    payload = json.dumps(result.to_dict(), indent=2, ensure_ascii=False)

    if args.output is not None:
        args.output.write_text(f"{payload}\n", encoding="utf-8")
    else:
        print(payload)
    return 0


def _resolve_debug_output_dir(
    *,
    list_url: str,
    resolved_url: str | None,
    dump_debug_output: bool,
    debug_output_dir: Path | None,
) -> Path | None:
    if debug_output_dir is not None:
        return debug_output_dir
    if not dump_debug_output:
        return None
    list_id = extract_list_id(resolved_url or "") or extract_list_id(list_url) or "unknown-list"
    return Path(os.getcwd()) / _DEFAULT_DEBUG_DIR_NAME / list_id
