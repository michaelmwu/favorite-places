"""Command-line interface for the GMaps scraper."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from gmaps_scraper.debug_dump import write_debug_dump
from gmaps_scraper.place_scraper import scrape_place
from gmaps_scraper.scraper import (
    DEFAULT_COLLECTION_MODE,
    BrowserSessionConfig,
    HttpSessionConfig,
    collect_saved_list_result,
)
from gmaps_scraper.url_tools import extract_list_id

_DEFAULT_DEBUG_DIR_NAME = ".gmaps-debug"


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", help="Google Maps list or place URL")
    parser.add_argument(
        "--kind",
        choices=["list", "place"],
        default="list",
        help="Scrape a list or an individual place page.",
    )
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
        help="Overall fetch timeout in milliseconds.",
    )
    parser.add_argument(
        "--settle-ms",
        type=int,
        default=3_000,
        help="Extra browser-only wait time after the page loads.",
    )
    parser.add_argument(
        "--fetch-mode",
        dest="collection_mode",
        choices=["auto", "curl", "browser"],
        default=DEFAULT_COLLECTION_MODE,
        help=(
            "Fetch mode: auto (curl_cffi with browser fallback), curl, or "
            "browser."
        ),
    )
    parser.add_argument(
        "--session-dir",
        type=Path,
        help="Reuse a persistent browser profile stored in this directory.",
    )
    parser.add_argument(
        "--proxy",
        default=os.environ.get("GMAPS_SCRAPER_PROXY"),
        help=(
            "Proxy URL passed through to curl_cffi and the browser. Prefer "
            "GMAPS_SCRAPER_PROXY for authenticated proxies so credentials "
            "do not appear in shell history or process listings."
        ),
    )
    parser.add_argument(
        "--http-cookie-jar",
        type=Path,
        help="Persist curl_cffi cookies in this Netscape-format cookie jar file.",
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
    browser_session = None
    if args.session_dir is not None or args.proxy is not None:
        browser_session = BrowserSessionConfig(
            profile_dir=args.session_dir,
            proxy=args.proxy,
        )
    http_session = None
    if args.http_cookie_jar is not None or args.proxy is not None:
        http_session = HttpSessionConfig(
            cookie_jar_path=args.http_cookie_jar,
            proxy=args.proxy,
        )

    if args.kind == "place":
        if args.collection_mode == "curl":
            parser.error(
                "Place scraping currently requires browser mode. "
                "Use `--fetch-mode browser`."
            )
        if args.debug_output_dir is not None or args.dump_debug_output:
            parser.error("Debug dump output is currently supported only for list scraping.")
        place_result = scrape_place(
            args.url,
            headless=not args.show_browser_window,
            timeout_ms=args.timeout_ms,
            settle_time_ms=args.settle_ms,
            browser_session=browser_session,
            http_session=http_session,
        )
        payload = json.dumps(place_result.to_dict(), indent=2, ensure_ascii=False)
        if args.output is not None:
            args.output.write_text(f"{payload}\n", encoding="utf-8")
        else:
            print(payload)
        return 0

    artifacts, result = collect_saved_list_result(
        args.url,
        headless=not args.show_browser_window,
        timeout_ms=args.timeout_ms,
        settle_time_ms=args.settle_ms,
        collection_mode=args.collection_mode,
        browser_session=browser_session,
        http_session=http_session,
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
