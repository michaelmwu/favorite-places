"""Command-line interface for the GMaps scraper."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from gmaps_scraper.debug_dump import write_debug_dump
from gmaps_scraper.models import PlaceDetails
from gmaps_scraper.place_scraper import scrape_place
from gmaps_scraper.scraper import (
    _HTTP_IMPERSONATE,
    DEFAULT_COLLECTION_MODE,
    BrowserSessionConfig,
    HttpSessionConfig,
    _import_curl_requests,
    _raise_for_status,
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
        "--download-photo",
        type=Path,
        help="For place scraping, download the representative photo to this file path.",
    )
    parser.add_argument(
        "--download-main-photo",
        type=Path,
        help="For place scraping, download the main place photo to this file path.",
    )
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
        if (
            args.download_photo is not None
            and args.output is not None
            and args.download_photo == args.output
        ):
            parser.error("`--download-photo` must be different from `--output`.")
        if (
            args.download_main_photo is not None
            and args.output is not None
            and args.download_main_photo == args.output
        ):
            parser.error("`--download-main-photo` must be different from `--output`.")
        if (
            args.download_photo is not None
            and args.download_main_photo is not None
            and args.download_photo == args.download_main_photo
        ):
            parser.error(
                "`--download-photo` and `--download-main-photo` must be different paths."
            )
        place_result = scrape_place(
            args.url,
            headless=not args.show_browser_window,
            timeout_ms=args.timeout_ms,
            settle_time_ms=args.settle_ms,
            browser_session=browser_session,
            http_session=http_session,
        )
        if args.download_photo is not None:
            try:
                _download_place_photo(
                    place_result,
                    output_path=args.download_photo,
                    http_session=http_session,
                )
            except RuntimeError as exc:
                parser.exit(1, f"{parser.prog}: error: {exc}\n")
        if args.download_main_photo is not None:
            try:
                _download_place_image(
                    place_result.main_photo_url,
                    output_path=args.download_main_photo,
                    http_session=http_session,
                    referer=place_result.resolved_url or place_result.source_url,
                    missing_message="No main photo URL was found for this place.",
                )
            except RuntimeError as exc:
                parser.exit(1, f"{parser.prog}: error: {exc}\n")
        payload = json.dumps(place_result.to_dict(), indent=2, ensure_ascii=False)
        if args.output is not None:
            args.output.write_text(f"{payload}\n", encoding="utf-8")
        else:
            print(payload)
        return 0
    if args.download_photo is not None or args.download_main_photo is not None:
        parser.error(
            "`--download-photo` and `--download-main-photo` are supported only with `--kind place`."
        )

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


def _download_place_photo(
    place_result: PlaceDetails,
    *,
    output_path: Path,
    http_session: HttpSessionConfig | None,
) -> None:
    _download_place_image(
        place_result.photo_url,
        output_path=output_path,
        http_session=http_session,
        referer=place_result.resolved_url or place_result.source_url,
        missing_message="No representative photo URL was found for this place.",
    )


def _download_place_image(
    photo_url: str | None,
    *,
    output_path: Path,
    http_session: HttpSessionConfig | None,
    referer: str,
    missing_message: str,
) -> None:
    if photo_url is None:
        raise RuntimeError(missing_message)
    try:
        curl_requests = _import_curl_requests()
        session_kwargs: dict[str, object] = {
            "impersonate": _HTTP_IMPERSONATE,
            "allow_redirects": True,
            "default_headers": True,
            "timeout": 30,
        }
        if http_session is not None and http_session.proxy is not None:
            session_kwargs["proxy"] = http_session.proxy

        with curl_requests.Session(**session_kwargs) as session:
            response = session.get(
                photo_url,
                referer=referer,
            )
            _raise_for_status(response)
            content = response.content

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(content)
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"Failed to download place photo: {exc}") from exc


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
