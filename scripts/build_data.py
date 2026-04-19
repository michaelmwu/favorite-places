from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import os
import random
import re
import shutil
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Any
from urllib.parse import unquote
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pycountry
from pydantic import TypeAdapter

ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "scripts" / "config" / "list_sources.json"
RAW_DIR = ROOT / "data" / "raw"
PLACES_CACHE_DIR = ROOT / "data" / "cache" / "google-places"
GENERATED_DIR = ROOT / "src" / "data" / "generated"
GENERATED_LISTS_DIR = GENERATED_DIR / "lists"
PUBLIC_DATA_DIR = ROOT / "public" / "data"
LIST_OVERRIDES_DIR = ROOT / "src" / "data" / "overrides" / "lists"
PLACE_OVERRIDES_DIR = ROOT / "src" / "data" / "overrides" / "places"
SCRAPER_STATE_DIR = ROOT / ".context" / "gmaps-scraper"
DEFAULT_REFRESH_WORKERS = 4
DEFAULT_REFRESH_RETRIES = 2
DEFAULT_REFRESH_RETRY_BACKOFF_SECONDS = 10.0
DEFAULT_REFRESH_STARTUP_JITTER_SECONDS = 8.0
SCRAPER_SESSION_SLOT_COUNT = 8
SCRAPER_SESSION_LOCK_WRITE_GRACE = timedelta(seconds=5)
SCRAPER_SESSION_MAX_AGE = timedelta(days=14)
ERROR_CACHE_TTL = timedelta(days=1)
UNMATCHED_CACHE_TTL = timedelta(days=3)
LOW_CONFIDENCE_CACHE_TTL = timedelta(days=3)
RATINGS_CACHE_TTL = timedelta(days=7)
NON_OPERATIONAL_CACHE_TTL = timedelta(days=3)
OPERATIONAL_CACHE_TTL = timedelta(days=14)
RAW_SOURCE_CACHE_TTL = timedelta(days=14)
RAW_SOURCE_REFRESH_JITTER = timedelta(days=3)
STRONG_MATCH_SCORE = 45
MAP_PIN_DISTANCE_WARNING_MIN_METERS = 100_000.0
MAP_PIN_DISTANCE_WARNING_BUFFER_METERS = 50_000.0
PLACES_TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
SCRAPER_BLOCK_ERROR_MARKERS = (
    "429",
    "403",
    "automated queries",
    "blocked",
    "captcha",
    "challenge",
    "denied",
    "forbidden",
    "rate limit",
    "rate-limit",
    "recaptcha",
    "robot",
    "sorry",
    "too many requests",
    "unusual traffic",
)
SCRAPER_SESSION_RESET_ERROR_MARKERS = SCRAPER_BLOCK_ERROR_MARKERS + (
    "failed to load http cookie jar",
)
COUNTRY_LOCALITY_ALIASES = (
    "England",
    "Scotland",
    "Wales",
    "Northern Ireland",
    "UK",
    "UAE",
    "USA",
    "Korea",
    "Taiwan",
    "Vatican City",
    "Ivory Coast",
)
COUNTRY_LOCALITY_KEYS: set[str] | None = None
PLACES_FIELD_MASK = ",".join(
    [
        "places.id",
        "places.name",
        "places.displayName",
        "places.formattedAddress",
        "places.googleMapsUri",
        "places.rating",
        "places.userRatingCount",
        "places.location",
        "places.primaryType",
        "places.primaryTypeDisplayName",
        "places.types",
        "places.businessStatus",
    ]
)
VIBE_TAG_KEYWORDS: dict[str, tuple[str, ...]] = {
    "cozy": (
        "cozy",
        "cosy",
        "warm",
        "intimate",
        "comfort",
        "homey",
        "home-y",
        "fireside",
    ),
    "quiet": (
        "quiet",
        "calm",
        "peaceful",
        "serene",
        "relaxing",
        "chill",
        "low-key",
        "low key",
        "library",
    ),
    "lively": (
        "lively",
        "buzzy",
        "busy",
        "fun",
        "energetic",
        "scene",
        "nightlife",
        "live music",
    ),
    "date-night": (
        "date night",
        "romantic",
        "intimate",
        "cocktail",
        "wine bar",
        "special occasion",
    ),
    "solo-friendly": (
        "solo",
        "counter",
        "bar seating",
        "quick bite",
        "book",
        "people watching",
    ),
    "group-friendly": (
        "group",
        "friends",
        "share",
        "shared",
        "family style",
        "large table",
    ),
    "laptop-friendly": (
        "laptop",
        "wifi",
        "wi-fi",
        "work",
        "cowork",
        "outlet",
        "outlets",
    ),
    "scenic": (
        "view",
        "views",
        "scenic",
        "waterfront",
        "beach",
        "mountain",
        "sunset",
        "rooftop",
        "garden",
        "park",
    ),
    "design-forward": (
        "design",
        "beautiful",
        "stylish",
        "interior",
        "architecture",
        "gallery",
        "aesthetic",
    ),
    "local-favorite": (
        "local favorite",
        "favorite",
        "institution",
        "neighborhood",
        "beloved",
        "regulars",
    ),
    "classic": (
        "classic",
        "old-school",
        "old school",
        "historic",
        "traditional",
        "since ",
        "heritage",
    ),
    "hidden-gem": (
        "hidden gem",
        "hidden",
        "underrated",
        "hole in the wall",
        "off the beaten",
        "tucked",
    ),
    "cheap-eats": (
        "cheap",
        "inexpensive",
        "affordable",
        "budget",
        "value",
        "cash only",
    ),
    "splurge": (
        "splurge",
        "expensive",
        "fine dining",
        "michelin",
        "omakase",
        "tasting menu",
        "luxury",
    ),
    "quick-stop": (
        "quick",
        "grab",
        "takeout",
        "take away",
        "snack",
        "bakery",
        "stand",
        "stall",
    ),
    "slow-afternoon": (
        "afternoon",
        "linger",
        "slow",
        "tea",
        "cafe",
        "coffee",
        "bookstore",
    ),
    "late-night": (
        "late night",
        "late-night",
        "24 hour",
        "24-hour",
        "bar",
        "pub",
        "izakaya",
        "night market",
    ),
    "outdoor-seating": (
        "outdoor",
        "patio",
        "terrace",
        "sidewalk",
        "courtyard",
        "garden",
    ),
    "rainy-day": (
        "rainy",
        "rain",
        "museum",
        "gallery",
        "bookstore",
        "indoor",
        "indoors",
    ),
    "family-friendly": (
        "family",
        "kids",
        "children",
        "playground",
        "stroller",
    ),
    "touristy-but-worth-it": (
        "touristy",
        "worth it",
        "iconic",
        "famous",
        "landmark",
        "must",
    ),
}
VIBE_CATEGORY_RULES: dict[str, tuple[str, ...]] = {
    "cafe": ("cozy", "solo-friendly", "slow-afternoon"),
    "coffee": ("cozy", "solo-friendly", "slow-afternoon"),
    "coffee-shop": ("cozy", "solo-friendly", "slow-afternoon"),
    "tea-house": ("quiet", "slow-afternoon"),
    "bakery": ("quick-stop", "slow-afternoon"),
    "bar": ("date-night", "late-night", "lively"),
    "pub": ("group-friendly", "late-night", "lively"),
    "night-club": ("late-night", "lively"),
    "restaurant": ("date-night", "group-friendly"),
    "fine-dining-restaurant": ("date-night", "splurge"),
    "museum": ("rainy-day", "solo-friendly"),
    "art-gallery": ("design-forward", "rainy-day"),
    "book-store": ("quiet", "rainy-day", "solo-friendly"),
    "park": ("scenic", "family-friendly"),
    "tourist-attraction": ("touristy-but-worth-it", "scenic"),
}
MARKER_ICON_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "cafe",
        (
            "cafe",
            "coffee",
            "coffee-shop",
            "coffee-roasters",
            "espresso-bar",
            "tea",
            "tea-house",
            "tea-room",
            "bubble-tea",
        ),
    ),
    (
        "bakery",
        (
            "bakery",
            "patisserie",
            "pastry",
            "pastry-shop",
            "dessert",
            "dessert-shop",
            "confectionery",
            "chocolate-shop",
            "ice-cream",
            "donut-shop",
        ),
    ),
    (
        "bar",
        (
            "bar",
            "pub",
            "cocktail-bar",
            "wine-bar",
            "beer-hall",
            "brewery",
            "sports-bar",
            "night-club",
            "izakaya",
            "karaoke",
            "live-music-venue",
        ),
    ),
    (
        "restaurant",
        (
            "restaurant",
            "food",
            "food-court",
            "meal-takeaway",
            "meal-delivery",
            "pizza",
            "ramen",
            "sushi",
            "noodle",
            "steakhouse",
            "barbecue",
            "brunch",
            "diner",
            "bistro",
            "cafeteria",
            "fast-food",
            "seafood",
            "tapas",
            "grill",
        ),
    ),
    (
        "museum",
        (
            "museum",
            "gallery",
            "library",
            "archive",
            "planetarium",
            "cultural-center",
        ),
    ),
    (
        "attraction",
        (
            "tourist-attraction",
            "landmark",
            "monument",
            "historical-landmark",
            "observation-deck",
            "visitor-center",
            "church",
            "cathedral",
            "basilica",
            "temple",
            "shrine",
            "mosque",
            "synagogue",
            "castle",
            "palace",
            "aquarium",
            "zoo",
            "amusement-park",
            "ferris-wheel",
        ),
    ),
    (
        "park",
        (
            "park",
            "botanical-garden",
            "garden",
            "hiking-area",
            "campground",
            "picnic-ground",
            "nature-preserve",
            "national-park",
            "trailhead",
        ),
    ),
    (
        "beach",
        (
            "beach",
            "marina",
            "waterfront",
            "pier",
            "swimming",
            "surf",
        ),
    ),
    (
        "shopping",
        (
            "store",
            "market",
            "shopping-mall",
            "gift-shop",
            "book-store",
            "clothing-store",
            "department-store",
            "antique-store",
        ),
    ),
    (
        "hotel",
        (
            "lodging",
            "hotel",
            "hostel",
            "resort",
            "inn",
            "ryokan",
        ),
    ),
    (
        "spa",
        (
            "spa",
            "sauna",
            "onsen",
            "massage",
            "hot-spring",
        ),
    ),
)
MARKER_ICON_TEXT_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "cafe",
        (
            "cafe",
            "coffee",
            "espresso",
            "kissaten",
            "tea",
            "matcha",
            "roastery",
        ),
    ),
    (
        "bakery",
        (
            "bakery",
            "baker",
            "bread",
            "pastry",
            "patisserie",
            "viennoiserie",
            "dessert",
            "gelato",
            "ice cream",
        ),
    ),
    (
        "bar",
        (
            "bar",
            "cocktail",
            "wine",
            "pub",
            "taproom",
            "speakeasy",
            "izakaya",
            "nightclub",
            "karaoke",
            "beer hall",
        ),
    ),
    (
        "restaurant",
        (
            "restaurant",
            "pizza",
            "ramen",
            "sushi",
            "udon",
            "soba",
            "yakitori",
            "yakiniku",
            "grill",
            "kitchen",
            "diner",
            "bistro",
            "trattoria",
            "osteria",
            "taqueria",
            "bbq",
            "burger",
            "curry",
            "noodle",
        ),
    ),
    (
        "museum",
        (
            "museum",
            "gallery",
            "archive",
            "exhibit",
            "library",
        ),
    ),
    (
        "attraction",
        (
            "temple",
            "shrine",
            "church",
            "cathedral",
            "mosque",
            "synagogue",
            "castle",
            "palace",
            "tower",
            "landmark",
            "monument",
            "observatory",
            "observation deck",
            "viewpoint",
            "park entrance",
        ),
    ),
    (
        "park",
        (
            "park",
            "garden",
            "forest",
            "hike",
            "trail",
        ),
    ),
    (
        "beach",
        (
            "beach",
            "coast",
            "bay",
            "surf",
            "island",
        ),
    ),
    (
        "shopping",
        (
            "shopping",
            "market",
            "store",
            "district",
            "shotengai",
            "mall",
            "bookstore",
        ),
    ),
    (
        "hotel",
        (
            "hotel",
            "hostel",
            "ryokan",
            "resort",
            "inn",
        ),
    ),
    (
        "spa",
        (
            "spa",
            "onsen",
            "sauna",
            "bathhouse",
            "massage",
        ),
    ),
)

try:
    from pipeline_models import (
        EnrichmentCacheEntry,
        EnrichmentPlace,
        Guide,
        GuideManifest,
        NormalizedPlace,
        PlacesSettings,
        RawPlace,
        RawSavedList,
        SourceConfig,
        PlaceField,
        PlaceProvenance,
    )
except ModuleNotFoundError:
    from scripts.pipeline_models import (
        EnrichmentCacheEntry,
        EnrichmentPlace,
        Guide,
        GuideManifest,
        NormalizedPlace,
        PlacesSettings,
        RawPlace,
        RawSavedList,
        SourceConfig,
        PlaceField,
        PlaceProvenance,
    )

try:
    from gmaps_scraper import (
        BrowserSessionConfig,
        HttpSessionConfig,
        ParseError,
        ScrapeError,
        scrape_place,
        scrape_saved_list,
    )
except ImportError:
    class ParseError(RuntimeError):
        pass

    class ScrapeError(RuntimeError):
        pass

    BrowserSessionConfig = None
    HttpSessionConfig = None
    scrape_place = None
    scrape_saved_list = None

RECOVERABLE_REFRESH_ERRORS = (ScrapeError, ParseError)


@dataclass(slots=True, frozen=True)
class ScraperSessionState:
    identity_key: str
    identity_dir: Path
    slot_key: str
    browser_profile_dir: Path
    http_cookie_jar_path: Path
    metadata_path: Path
    lock_path: Path


def current_scraper_proxy() -> str | None:
    value = os.environ.get("GMAPS_SCRAPER_PROXY")
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    return normalized


def scraper_session_identity_key(proxy: str | None) -> str:
    if proxy is None:
        return "direct"
    digest = hashlib.sha256(proxy.encode("utf-8")).hexdigest()[:16]
    return f"proxy-{digest}"


def build_scraper_session_state(
    proxy: str | None,
    *,
    slot_key: str = "slot-0",
    lock_path: Path | None = None,
) -> ScraperSessionState:
    identity_key = scraper_session_identity_key(proxy)
    identity_dir = SCRAPER_STATE_DIR / identity_key
    return ScraperSessionState(
        identity_key=identity_key,
        identity_dir=identity_dir,
        slot_key=slot_key,
        browser_profile_dir=identity_dir / "browser" / slot_key,
        http_cookie_jar_path=identity_dir / f"http-cookies.{slot_key}.txt",
        metadata_path=identity_dir / f"metadata.{slot_key}.json",
        lock_path=lock_path or identity_dir / "locks" / f"{slot_key}.lock",
    )


def ensure_scraper_session_state(
    proxy: str | None,
    *,
    now: datetime | None = None,
) -> ScraperSessionState:
    reference_time = now or datetime.now(UTC)
    identity_key = scraper_session_identity_key(proxy)
    identity_dir = SCRAPER_STATE_DIR / identity_key
    sweep_stale_scraper_session_states(proxy, identity_dir=identity_dir, now=reference_time)
    slot_key, lock_path = acquire_scraper_session_slot(identity_dir)
    state = build_scraper_session_state(
        proxy,
        slot_key=slot_key,
        lock_path=lock_path,
    )
    if scraper_session_is_stale(state, now=reference_time):
        clear_scraper_session_state(state)
    state.identity_dir.mkdir(parents=True, exist_ok=True)
    return state


def load_scraper_session_metadata(state: ScraperSessionState) -> dict[str, str] | None:
    if not state.metadata_path.exists():
        return None
    try:
        payload = json.loads(state.metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    metadata = {key: value for key, value in payload.items() if isinstance(key, str) and isinstance(value, str)}
    return metadata or None


def scraper_session_is_stale(
    state: ScraperSessionState,
    *,
    now: datetime | None = None,
) -> bool:
    metadata = load_scraper_session_metadata(state)
    if metadata is None:
        return (
            state.metadata_path.exists()
            or state.browser_profile_dir.exists()
            or state.http_cookie_jar_path.exists()
        )
    timestamp = metadata.get("last_used_at") or metadata.get("created_at")
    if timestamp is None:
        return True
    try:
        last_used_at = parse_metadata_datetime(timestamp)
    except ValueError:
        return True
    reference_time = now or datetime.now(UTC)
    return reference_time - last_used_at >= SCRAPER_SESSION_MAX_AGE


def record_scraper_session_use(
    state: ScraperSessionState,
    *,
    proxy: str | None,
    now: datetime | None = None,
) -> None:
    state.identity_dir.mkdir(parents=True, exist_ok=True)
    existing = load_scraper_session_metadata(state) or {}
    timestamp = (now or datetime.now(UTC)).isoformat()
    payload = {
        "identity_key": state.identity_key,
        "created_at": existing.get("created_at", timestamp),
        "last_used_at": timestamp,
        "proxy_kind": "proxy" if proxy is not None else "direct",
    }
    state.metadata_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def clear_scraper_session_state(state: ScraperSessionState) -> None:
    if state.browser_profile_dir.exists() and state.browser_profile_dir.is_dir():
        shutil.rmtree(state.browser_profile_dir, ignore_errors=True)

    for path in (state.http_cookie_jar_path, state.metadata_path):
        try:
            if path.exists() or path.is_symlink():
                path.unlink()
        except OSError:
            pass

    for directory in (state.browser_profile_dir.parent, state.identity_dir):
        try:
            if directory.exists() and not any(directory.iterdir()):
                directory.rmdir()
        except OSError:
            pass


def release_scraper_session_lock(state: ScraperSessionState) -> None:
    try:
        if state.lock_path.exists() or state.lock_path.is_symlink():
            state.lock_path.unlink()
    except OSError:
        pass

    for directory in (state.lock_path.parent, state.identity_dir):
        try:
            if directory.exists() and not any(directory.iterdir()):
                directory.rmdir()
        except OSError:
            pass


def should_reset_scraper_session(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(marker in message for marker in SCRAPER_SESSION_RESET_ERROR_MARKERS)


def sweep_stale_scraper_session_states(
    proxy: str | None,
    *,
    identity_dir: Path,
    now: datetime,
) -> None:
    for metadata_path in sorted(identity_dir.glob("metadata.*.json")):
        slot_key = metadata_path.stem.removeprefix("metadata.")
        state = build_scraper_session_state(proxy, slot_key=slot_key)
        if scraper_session_lock_is_active(state.lock_path):
            continue
        if scraper_session_is_stale(state, now=now):
            clear_scraper_session_state(state)
            release_scraper_session_lock(state)


def acquire_scraper_session_slot(identity_dir: Path) -> tuple[str, Path]:
    locks_dir = identity_dir / "locks"
    locks_dir.mkdir(parents=True, exist_ok=True)
    for index in range(SCRAPER_SESSION_SLOT_COUNT):
        slot_key = f"slot-{index}"
        lock_path = locks_dir / f"{slot_key}.lock"
        if try_acquire_scraper_session_lock(lock_path):
            return slot_key, lock_path

    slot_key = f"slot-overflow-{os.getpid()}"
    lock_path = locks_dir / f"{slot_key}.lock"
    if try_acquire_scraper_session_lock(lock_path):
        return slot_key, lock_path
    raise RuntimeError(f"Could not allocate a scraper session slot under {identity_dir}.")


def try_acquire_scraper_session_lock(lock_path: Path) -> bool:
    payload = f"{os.getpid()}\n"
    for _ in range(2):
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            if not scraper_session_lock_is_active(lock_path):
                try:
                    lock_path.unlink()
                except OSError:
                    return False
                continue
            return False
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(payload)
        except Exception:
            try:
                lock_path.unlink()
            except OSError:
                pass
            raise
        return True
    return False


def scraper_session_lock_is_active(lock_path: Path) -> bool:
    if not lock_path.exists():
        return False
    try:
        owner_text = lock_path.read_text(encoding="utf-8").strip()
        owner_pid = int(owner_text)
    except OSError:
        return True
    except ValueError:
        try:
            modified_at = datetime.fromtimestamp(lock_path.stat().st_mtime, tz=UTC)
        except OSError:
            return True
        return datetime.now(UTC) - modified_at < SCRAPER_SESSION_LOCK_WRITE_GRACE
    try:
        os.kill(owner_pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def build_scraper_sessions(
    proxy: str | None,
    *,
    now: datetime | None = None,
) -> tuple[ScraperSessionState, Any, Any]:
    session_state = ensure_scraper_session_state(proxy, now=now)
    return session_state, *build_scraper_configs(session_state, proxy)


def build_scraper_configs(
    session_state: ScraperSessionState,
    proxy: str | None,
) -> tuple[Any, Any]:
    browser_session = None
    http_session = None
    if BrowserSessionConfig is not None:
        browser_session = BrowserSessionConfig(
            profile_dir=session_state.browser_profile_dir,
            proxy=proxy,
        )
    if HttpSessionConfig is not None:
        http_session = HttpSessionConfig(
            cookie_jar_path=session_state.http_cookie_jar_path,
            proxy=proxy,
        )
    return browser_session, http_session


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Refresh every configured source before rebuilding generated JSON.",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Run the scraper in headed mode.",
    )
    parser.add_argument(
        "--refresh-force",
        action="store_true",
        help="Force-refresh raw sources even if a local file import is unchanged.",
    )
    parser.add_argument(
        "--refresh-list",
        action="append",
        default=[],
        help="Refresh only the configured source matching this slug, URL, or path. Repeat to target multiple sources.",
    )
    parser.add_argument(
        "--refresh-workers",
        type=int,
        default=DEFAULT_REFRESH_WORKERS,
        help="Maximum parallel workers for headless raw list refreshes. Headed refreshes stay single-worker.",
    )
    parser.add_argument(
        "--refresh-retries",
        type=non_negative_int,
        default=DEFAULT_REFRESH_RETRIES,
        help="Retry each Google list refresh this many times after the first failed attempt.",
    )
    parser.add_argument(
        "--refresh-retry-backoff-seconds",
        type=non_negative_float,
        default=DEFAULT_REFRESH_RETRY_BACKOFF_SECONDS,
        help="Initial delay before retrying a failed Google list refresh. Later retries use exponential backoff.",
    )
    parser.add_argument(
        "--refresh-startup-jitter-seconds",
        type=non_negative_float,
        default=DEFAULT_REFRESH_STARTUP_JITTER_SECONDS,
        help="Maximum randomized delay before each Google list scrape attempt starts its browser.",
    )
    parser.add_argument(
        "--enrich",
        action="store_true",
        help="Fill missing or stale place enrichment cache entries from Google Maps place pages, with Places API fallback when configured.",
    )
    parser.add_argument(
        "--refresh-enrichment",
        action="store_true",
        help="Force-refresh place enrichment cache entries for every place.",
    )
    return parser


def non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return parsed


def non_negative_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return parsed


def load_sources() -> list[SourceConfig]:
    if not CONFIG_PATH.exists():
        return []
    sources = TypeAdapter(list[SourceConfig]).validate_json(CONFIG_PATH.read_text(encoding="utf-8"))
    validate_unique_source_slugs(sources)
    return sources


def validate_unique_source_slugs(sources: list[SourceConfig]) -> None:
    slug_counts = Counter(source.slug for source in sources)
    duplicate_slugs = sorted(slug for slug, count in slug_counts.items() if count > 1)
    if not duplicate_slugs:
        return

    duplicate_text = ", ".join(duplicate_slugs)
    raise RuntimeError(f"Duplicate source slug(s) in {CONFIG_PATH}: {duplicate_text}")


def refresh_raw_sources(
    *,
    headed: bool,
    force_refresh: bool,
    refresh_lists: list[str],
    refresh_workers: int,
    refresh_retries: int = DEFAULT_REFRESH_RETRIES,
    refresh_retry_backoff_seconds: float = DEFAULT_REFRESH_RETRY_BACKOFF_SECONDS,
    refresh_startup_jitter_seconds: float = DEFAULT_REFRESH_STARTUP_JITTER_SECONDS,
) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    sources = load_sources()
    selected_sources = resolve_refresh_sources(sources, refresh_lists)
    selected_slugs = {source.slug for source in selected_sources}
    refresh_jobs: list[tuple[SourceConfig, Path, bool]] = []

    for source in sources:
        if selected_sources and source.slug not in selected_slugs:
            continue

        raw_path = RAW_DIR / f"{source.slug}.json"
        existing_payload = load_raw_saved_list(raw_path)

        if source.type == "google_export_csv":
            payload = refresh_google_export_csv(
                source,
                existing_payload=existing_payload,
                force_refresh=force_refresh or bool(selected_sources),
            )
            if payload is not None:
                write_json(raw_path, payload)
            continue

        source_url = source.url
        if not source_url:
            raise RuntimeError(f"Configured source {source.slug} is missing a URL.")

        refresh_reason = (
            None
            if force_refresh or bool(selected_sources)
            else raw_source_refresh_reason(source, existing_payload)
        )
        if not force_refresh and not bool(selected_sources) and refresh_reason is None:
            print(f"Skipping {source.slug} (raw snapshot fresh)")
            continue

        if force_refresh:
            print(f"Refreshing {source.slug} from {source_url} (forced)")
        elif selected_sources:
            print(f"Refreshing {source.slug} from {source_url} (selected)")
        else:
            print(f"Refreshing {source.slug} from {source_url} ({refresh_reason})")
        backup_available = existing_payload is not None and raw_source_signature_matches(
            source,
            existing_payload.source_signature,
        )
        refresh_jobs.append((source, raw_path, backup_available))

    if not refresh_jobs:
        return

    effective_startup_jitter_seconds = (
        refresh_startup_jitter_seconds if not headed and len(refresh_jobs) > 1 else 0
    )
    max_workers = max(1, refresh_workers)
    if headed or len(refresh_jobs) == 1 or max_workers == 1:
        failures: list[str] = []
        for source, raw_path, backup_available in refresh_jobs:
            try:
                payload = scrape_google_list_url_with_retries(
                    source,
                    headed=headed,
                    refresh_retries=refresh_retries,
                    refresh_retry_backoff_seconds=refresh_retry_backoff_seconds,
                    refresh_startup_jitter_seconds=effective_startup_jitter_seconds,
                )
            except RECOVERABLE_REFRESH_ERRORS as exc:
                if backup_available:
                    print(f"Keeping existing raw snapshot for {source.slug} after refresh failure: {exc}")
                    continue
                failures.append(f"{source.slug}: {exc}")
                continue
            write_json(raw_path, payload)
        if failures:
            failure_text = "\n".join(failures)
            raise RuntimeError(f"Raw refresh failed for {len(failures)} source(s):\n{failure_text}")
        return

    max_workers = min(max_workers, len(refresh_jobs))
    print(f"Running {len(refresh_jobs)} headless refresh jobs with {max_workers} workers")
    failures: list[str] = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(
                scrape_google_list_url_with_retries,
                source,
                headed=False,
                refresh_retries=refresh_retries,
                refresh_retry_backoff_seconds=refresh_retry_backoff_seconds,
                refresh_startup_jitter_seconds=effective_startup_jitter_seconds,
            ): (source, raw_path, backup_available)
            for source, raw_path, backup_available in refresh_jobs
        }
        for future in as_completed(future_map):
            source, raw_path, backup_available = future_map[future]
            try:
                payload = future.result()
            except RECOVERABLE_REFRESH_ERRORS as exc:
                if backup_available:
                    print(f"Keeping existing raw snapshot for {source.slug} after refresh failure: {exc}")
                else:
                    failures.append(f"{source.slug}: {exc}")
                continue
            write_json(raw_path, payload)

    if failures:
        failure_text = "\n".join(failures)
        raise RuntimeError(f"Raw refresh failed for {len(failures)} source(s):\n{failure_text}")


def scrape_google_list_url(source: SourceConfig, *, headed: bool) -> RawSavedList:
    if scrape_saved_list is None:
        raise RuntimeError(
            "Could not import gmaps_scraper. "
            "Run `uv sync` to install the scraper dependency before using --refresh."
        )

    source_url = source.url
    if not source_url:
        raise RuntimeError(f"Configured source {source.slug} is missing a URL.")

    proxy = current_scraper_proxy()
    session_state, browser_session, http_session = build_scraper_sessions(proxy)
    try:
        try:
            result = scrape_saved_list(
                source_url,
                headless=not headed,
                collection_mode="curl",
                browser_session=browser_session,
                http_session=http_session,
            )
        except RECOVERABLE_REFRESH_ERRORS as exc:
            if should_reset_scraper_session(exc):
                clear_scraper_session_state(session_state)
                browser_session, http_session = build_scraper_configs(session_state, proxy)
            result = scrape_saved_list(
                source_url,
                headless=not headed,
                collection_mode="browser",
                browser_session=browser_session,
                http_session=http_session,
            )

        record_scraper_session_use(session_state, proxy=proxy)
        payload = RawSavedList.model_validate(result.to_dict())
        if source.title and not payload.title:
            payload.title = source.title
        stamp_raw_saved_list(payload, source, source_signature=raw_source_signature(source))
        return payload
    finally:
        release_scraper_session_lock(session_state)


def scrape_google_list_url_with_retries(
    source: SourceConfig,
    *,
    headed: bool,
    refresh_retries: int,
    refresh_retry_backoff_seconds: float,
    refresh_startup_jitter_seconds: float,
) -> RawSavedList:
    attempts = max(1, refresh_retries + 1)
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        sleep_for_refresh_startup_jitter(refresh_startup_jitter_seconds)
        try:
            return scrape_google_list_url(source, headed=headed)
        except RECOVERABLE_REFRESH_ERRORS as exc:
            last_error = exc
            if attempt >= attempts:
                break

            backoff_seconds = max(0.0, refresh_retry_backoff_seconds) * (2 ** (attempt - 1))
            print(
                f"Retrying {source.slug} after refresh failure "
                f"({attempt}/{attempts - 1} retries used): {exc}"
            )
            if backoff_seconds > 0:
                time.sleep(backoff_seconds)

    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Refresh did not produce a result for {source.slug}.")


def sleep_for_refresh_startup_jitter(max_seconds: float) -> None:
    if max_seconds <= 0:
        return

    delay = random.uniform(0, max_seconds)
    if delay > 0:
        time.sleep(delay)


def refresh_google_export_csv(
    source: SourceConfig,
    *,
    existing_payload: RawSavedList | None,
    force_refresh: bool,
) -> RawSavedList | None:
    source_signature = raw_source_signature(source)
    if not force_refresh and existing_payload is not None and existing_payload.source_signature == source_signature:
        print(f"Skipping {source.slug} (csv unchanged)")
        return None

    csv_path = configured_source_path(source)
    action = "Re-importing" if force_refresh else "Importing"
    print(f"{action} {source.slug} from {csv_path}")
    payload = import_saved_list_csv(source)
    stamp_raw_saved_list(payload, source, source_signature=source_signature)
    return payload


def rebuild_generated_data() -> None:
    sync_local_csv_sources()
    GENERATED_LISTS_DIR.mkdir(parents=True, exist_ok=True)
    PUBLIC_DATA_DIR.mkdir(parents=True, exist_ok=True)
    guides: list[Guide] = []

    for raw_path in sorted(RAW_DIR.glob("*.json")):
        raw = RawSavedList.model_validate_json(raw_path.read_text(encoding="utf-8"))
        enrichment_cache = load_places_cache(raw_path.stem)
        guide = normalize_guide(raw_path.stem, raw, enrichment_cache=enrichment_cache)
        guides.append(guide)
        write_json(GENERATED_LISTS_DIR / f"{guide.slug}.json", guide)

    guides.sort(key=lambda guide: (guide.country_name, guide.city_name, guide.title))
    manifests = [summarize_guide(guide) for guide in guides]
    search_index = build_search_index(guides)
    write_json(GENERATED_DIR / "manifests.json", manifests)
    write_json(GENERATED_DIR / "search-index.json", search_index)
    write_json(PUBLIC_DATA_DIR / "search-index.json", search_index, compact=True)


def sync_local_csv_sources() -> None:
    for source in load_sources():
        if source.type != "google_export_csv":
            continue

        raw_path = RAW_DIR / f"{source.slug}.json"
        existing_payload = load_raw_saved_list(raw_path)
        payload = refresh_google_export_csv(
            source,
            existing_payload=existing_payload,
            force_refresh=False,
        )
        if payload is None:
            continue

        write_json(raw_path, payload)


def import_saved_list_csv(source: SourceConfig) -> RawSavedList:
    csv_path = configured_source_path(source)
    rows = parse_google_export_rows(csv_path)
    header_index = find_google_export_header_index(rows)
    if header_index is None:
        raise RuntimeError(f"Could not find Google export headers in {csv_path}")

    headers = [normalize_csv_header(cell) for cell in rows[header_index]]
    description = csv_preamble_description(rows[:header_index])
    places: list[RawPlace] = []

    for row in rows[header_index + 1 :]:
        if not any(cell.strip() for cell in row):
            continue
        row_values = {header: value.strip() for header, value in zip(headers, row, strict=False)}
        maps_url = as_string(row_values.get("url"))
        if maps_url is None:
            continue

        url_name = name_from_maps_url(maps_url)
        raw_title = as_string(row_values.get("title"))
        note = combine_place_note(row_values.get("note"), row_values.get("comment"))
        places.append(
            RawPlace(
                name=url_name or raw_title or "Saved place",
                note=note,
                maps_url=maps_url,
                maps_place_token=extract_maps_place_token(maps_url),
            )
        )

    return RawSavedList(
        title=source.title or fallback_source_title(source),
        description=description,
        places=places,
    )


def parse_google_export_rows(path: Path) -> list[list[str]]:
    text = path.read_text(encoding="utf-8-sig", errors="replace")
    for delimiter in ("\t", ",", ";"):
        rows = list(csv.reader(io.StringIO(text), delimiter=delimiter))
        if find_google_export_header_index(rows) is not None:
            return rows
    raise RuntimeError(f"Could not detect a supported delimiter for {path}")


def find_google_export_header_index(rows: list[list[str]]) -> int | None:
    for index, row in enumerate(rows):
        normalized_row = [normalize_csv_header(cell) for cell in row]
        if normalized_row[:5] == ["title", "note", "url", "tags", "comment"]:
            return index
        if normalized_row[:3] == ["title", "note", "url"]:
            return index
    return None


def normalize_csv_header(value: str) -> str:
    return value.strip().lower()


def csv_preamble_description(rows: list[list[str]]) -> str | None:
    lines = [" ".join(cell.strip() for cell in row if cell.strip()) for row in rows]
    lines = [line for line in lines if line]
    if not lines:
        return None
    return "\n".join(lines)


def combine_place_note(note: Any, comment: Any) -> str | None:
    note_parts = [as_string(note), as_string(comment)]
    combined = [part for part in note_parts if part]
    if not combined:
        return None
    return "\n\n".join(combined)


def name_from_maps_url(maps_url: str) -> str | None:
    match = re.search(r"/place/([^/]+)/data=", maps_url)
    if not match:
        return None
    candidate = unquote(match.group(1).replace("+", " "))
    return as_string(candidate)


def extract_maps_cid(maps_url: str | None) -> str | None:
    if maps_url is None:
        return None
    match = re.search(r"[?&]cid=(\d+)", maps_url)
    if not match:
        return None
    return match.group(1)


def extract_maps_place_token(maps_url: str | None) -> str | None:
    if maps_url is None:
        return None
    match = re.search(r"(0x[0-9a-fA-F]+:0x[0-9a-fA-F]+)", maps_url)
    if not match:
        return None
    return match.group(1).lower()


def normalize_guide(slug: str, raw: RawSavedList, *, enrichment_cache: dict[str, EnrichmentCacheEntry]) -> Guide:
    list_override = read_json(LIST_OVERRIDES_DIR / f"{slug}.json")
    place_override_map = read_json(PLACE_OVERRIDES_DIR / f"{slug}.json")

    title = as_string(list_override.get("title")) or raw.title or slug.replace("-", " ").title()
    description = as_string(list_override.get("description")) or raw.description

    city_name = as_string(list_override.get("city_name")) or infer_city_name(title)
    country_name = as_string(list_override.get("country_name")) or infer_country_name(title, raw)
    country_code = as_string(list_override.get("country_code")) or infer_country_code(country_name)

    description_tags = extract_hashtags(description)
    override_tags = coerce_string_list(list_override.get("list_tags"))
    list_tags = sorted({*description_tags, *override_tags})

    normalized_places: list[NormalizedPlace] = []
    category_counter: Counter[str] = Counter()
    prefer_enrichment_names = raw.configured_source_type == "google_export_csv"

    for place in raw.places:
        place_id = stable_place_id(place, source_type=raw.configured_source_type)
        override = place_override_map.get(place_id, {})
        enrichment_cache_entry = enrichment_cache.get(place_id)
        enrichment = coerce_enrichment_place(enrichment_cache_entry)
        primary_category = (
            as_string(override.get("primary_category"))
            or enrichment.primary_type_display_name
            or enrichment.primary_type
        )
        tags = sorted(
            {
                *coerce_string_list(override.get("tags")),
                *derive_place_tags(place, city_name, enrichment=enrichment, category=primary_category),
            }
        )
        neighborhood = as_string(override.get("neighborhood")) or infer_neighborhood(
            place.address,
            city_name=city_name,
        )
        hidden = bool(override.get("hidden", False))
        top_pick_override = as_bool(override.get("top_pick"))
        top_pick = top_pick_override if top_pick_override is not None else place.is_favorite
        note = as_string(override.get("note")) or place.note
        why_recommended = as_string(override.get("why_recommended"))
        if "vibe_tags" in override:
            override_vibe_tags = coerce_string_list(override.get("vibe_tags"))
            vibe_tags = sorted({slugify(tag) for tag in override_vibe_tags if slugify(tag)})
        else:
            vibe_tags = derive_vibe_tags(
                place,
                enrichment=enrichment,
                category=primary_category,
                tags=tags,
                note=note,
                why_recommended=why_recommended,
                top_pick=top_pick,
            )
        marker_icon = derive_marker_icon(
            place,
            enrichment=enrichment,
            category=primary_category,
            tags=tags,
            note=note,
            why_recommended=why_recommended,
        )
        manual_rank = as_int(override.get("manual_rank")) or 0
        status = (
            as_string(override.get("status"))
            or normalize_business_status(enrichment.business_status)
            or "active"
        )
        preferred_name = place.name
        if prefer_enrichment_names and enrichment.display_name:
            preferred_name = enrichment.display_name

        normalized = NormalizedPlace(
            id=place_id,
            name=as_string(override.get("name")) or preferred_name,
            address=place.address or enrichment.formatted_address,
            lat=place.lat,
            lng=place.lng,
            maps_url=enrichment.google_maps_uri or place.maps_url,
            cid=place.cid,
            google_id=place.google_id,
            google_place_id=enrichment.google_place_id,
            google_place_resource_name=enrichment.google_place_resource_name,
            primary_category=primary_category,
            marker_icon=marker_icon,
            tags=tags,
            vibe_tags=vibe_tags,
            neighborhood=neighborhood,
            note=note,
            why_recommended=why_recommended,
            top_pick=top_pick,
            hidden=hidden,
            manual_rank=manual_rank,
            status=status,
        )
        normalized.provenance = build_place_provenance(
            raw=raw,
            raw_place=place,
            override=override,
            normalized=normalized,
            enrichment_cache_entry=enrichment_cache_entry,
            enrichment=enrichment,
            primary_category=primary_category,
            tags=tags,
            city_name=city_name,
            top_pick_override=top_pick_override,
            status=status,
            prefer_enrichment_names=prefer_enrichment_names,
        )
        normalized_places.append(normalized)
        if primary_category:
            category_counter[primary_category] += 1

    normalized_places.sort(
        key=lambda place: (
            place.hidden,
            not place.top_pick,
            -place.manual_rank,
            place.name.lower(),
        )
    )

    featured_place_ids = [
        place_id
        for place_id in coerce_string_list(list_override.get("featured_place_ids"))
        if any(place.id == place_id for place in normalized_places)
    ]
    auto_featured_ids = [place.id for place in normalized_places if place.top_pick]
    if featured_place_ids:
        featured_place_ids = list(dict.fromkeys([*featured_place_ids, *auto_featured_ids]))[:3]
    else:
        featured_place_ids = auto_featured_ids[:3]

    top_categories = [name for name, _count in category_counter.most_common(4)]
    generated_at = datetime.now(UTC).isoformat()
    visible_places = [place for place in normalized_places if not place.hidden]
    guide_center = guide_location_center(visible_places)
    warn_far_map_pins(slug, visible_places, guide_center)

    return Guide(
        slug=slug,
        title=title,
        description=description,
        source_url=raw.source_url,
        list_id=raw.list_id,
        country_name=country_name or "Unknown",
        country_code=country_code,
        city_name=city_name or title,
        list_tags=list_tags,
        featured_place_ids=featured_place_ids,
        top_categories=top_categories,
        generated_at=generated_at,
        place_count=len(visible_places),
        center_lat=guide_center[0],
        center_lng=guide_center[1],
        places=normalized_places,
    )


def build_place_provenance(
    *,
    raw: RawSavedList,
    raw_place: RawPlace,
    override: dict[str, Any],
    normalized: NormalizedPlace,
    enrichment_cache_entry: EnrichmentCacheEntry | None,
    enrichment: EnrichmentPlace,
    primary_category: str | None,
    tags: list[str],
    city_name: str | None,
    top_pick_override: bool | None,
    status: str,
    prefer_enrichment_names: bool,
) -> PlaceProvenance:
    manual_name = as_string(override.get("name"))
    manual_category = as_string(override.get("primary_category"))
    manual_note = as_string(override.get("note"))
    manual_neighborhood = as_string(override.get("neighborhood"))
    manual_why_recommended = as_string(override.get("why_recommended"))
    manual_status = as_string(override.get("status"))

    provenance = PlaceProvenance()
    provenance.name = (
        manual_place_field(normalized.name)
        if manual_name
        else google_places_field(normalized.name, enrichment_cache_entry)
        if prefer_enrichment_names and enrichment.display_name
        else google_list_field(normalized.name, raw)
    )
    if normalized.address:
        provenance.address = (
            google_list_field(normalized.address, raw)
            if raw_place.address
            else google_places_field(normalized.address, enrichment_cache_entry)
        )
    if normalized.lat is not None:
        provenance.lat = google_list_field(normalized.lat, raw)
    if normalized.lng is not None:
        provenance.lng = google_list_field(normalized.lng, raw)
    provenance.maps_url = (
        google_places_field(normalized.maps_url, enrichment_cache_entry)
        if enrichment.google_maps_uri
        else google_list_field(normalized.maps_url, raw)
    )
    if normalized.cid:
        provenance.cid = google_list_field(normalized.cid, raw)
    if normalized.google_id:
        provenance.google_id = google_list_field(normalized.google_id, raw)
    if normalized.google_place_id:
        provenance.google_place_id = google_places_field(normalized.google_place_id, enrichment_cache_entry)
    if normalized.google_place_resource_name:
        provenance.google_place_resource_name = google_places_field(
            normalized.google_place_resource_name,
            enrichment_cache_entry,
        )
    if primary_category:
        provenance.primary_category = (
            manual_place_field(primary_category)
            if manual_category
            else google_places_field(primary_category, enrichment_cache_entry)
        )
    provenance.tags = build_tag_provenance(
        raw=raw,
        raw_place=raw_place,
        override=override,
        enrichment_cache_entry=enrichment_cache_entry,
        enrichment=enrichment,
        primary_category=primary_category,
        primary_category_field=provenance.primary_category,
        city_name=city_name,
        tags=tags,
    )
    if normalized.neighborhood:
        provenance.neighborhood = (
            manual_place_field(normalized.neighborhood)
            if manual_neighborhood
            else google_list_field(normalized.neighborhood, raw)
        )
    if normalized.note:
        provenance.note = manual_place_field(normalized.note) if manual_note else google_list_field(normalized.note, raw)
    if normalized.why_recommended:
        provenance.why_recommended = (
            manual_place_field(normalized.why_recommended)
            if manual_why_recommended
            else None
        )
    provenance.top_pick = (
        manual_place_field(normalized.top_pick)
        if top_pick_override is not None
        else google_list_field(normalized.top_pick, raw)
    )
    if "hidden" in override:
        provenance.hidden = manual_place_field(normalized.hidden)
    if "manual_rank" in override:
        provenance.manual_rank = manual_place_field(normalized.manual_rank)
    if manual_status:
        provenance.status = manual_place_field(status)
    elif enrichment.business_status:
        provenance.status = google_places_field(status, enrichment_cache_entry)
    return provenance


def build_tag_provenance(
    *,
    raw: RawSavedList,
    raw_place: RawPlace,
    override: dict[str, Any],
    enrichment_cache_entry: EnrichmentCacheEntry | None,
    enrichment: EnrichmentPlace,
    primary_category: str | None,
    primary_category_field: PlaceField | None,
    city_name: str | None,
    tags: list[str],
) -> list[PlaceField]:
    ranked_fields: dict[str, tuple[int, PlaceField]] = {}

    def put_tag(value: str | None, field: PlaceField | None, *, priority: int) -> None:
        tag = as_string(value)
        if tag is None or field is None:
            return
        existing = ranked_fields.get(tag)
        if existing is None or existing[0] < priority:
            ranked_fields[tag] = (priority, field)

    for tag in coerce_string_list(override.get("tags")):
        put_tag(tag, manual_place_field(tag), priority=30)
    if city_name:
        tag = slugify(city_name)
        put_tag(tag, google_list_field(tag, raw), priority=10)
    for locality in infer_address_localities(raw_place.address, city_name=city_name):
        tag = slugify(locality)
        put_tag(tag, google_list_field(tag, raw), priority=10)
    if primary_category:
        tag = slugify(primary_category)
        put_tag(
            tag,
            source_place_field(tag, primary_category_field),
            priority=30 if primary_category_field and primary_category_field.source == "manual" else 20,
        )
    for place_type in enrichment.types[:4]:
        tag = slugify(place_type.replace("_", "-"))
        put_tag(tag, google_places_field(tag, enrichment_cache_entry), priority=20)

    return [ranked_fields[tag][1] for tag in tags if tag in ranked_fields]


def source_place_field(value: Any, source_field: PlaceField | None) -> PlaceField | None:
    if source_field is None:
        return None
    return PlaceField(
        value=value,
        source=source_field.source,
        fetched_at=source_field.fetched_at,
        expires_at=source_field.expires_at,
    )


def manual_place_field(value: Any) -> PlaceField:
    return PlaceField(value=value, source="manual")


def google_list_field(value: Any, raw: RawSavedList) -> PlaceField:
    return PlaceField(
        value=value,
        source="google_list",
        fetched_at=raw.fetched_at,
        expires_at=raw.refresh_after,
    )


def google_places_field(value: Any, cache_entry: EnrichmentCacheEntry | None) -> PlaceField:
    source = "google_places"
    if cache_entry is not None and cache_entry.source == "google_maps_page":
        source = "google_maps_page"
    return PlaceField(
        value=value,
        source=source,
        fetched_at=cache_entry.fetched_at if cache_entry else None,
        expires_at=cache_entry.refresh_after if cache_entry else None,
    )


def summarize_guide(guide: Guide) -> GuideManifest:
    featured_names = [
        place.name
        for place in guide.places
        if place.id in set(guide.featured_place_ids) and not place.hidden
    ]
    return GuideManifest(
        slug=guide.slug,
        title=guide.title,
        description=guide.description,
        country_name=guide.country_name,
        country_code=guide.country_code,
        center_lat=guide.center_lat,
        center_lng=guide.center_lng,
        city_name=guide.city_name,
        list_tags=guide.list_tags,
        place_count=guide.place_count,
        featured_names=featured_names[:3],
        top_categories=guide.top_categories,
    )


def guide_location_center(places: list[NormalizedPlace]) -> tuple[float | None, float | None]:
    coordinates = [
        (place.lat, place.lng)
        for place in places
        if place.lat is not None and place.lng is not None
    ]
    if not coordinates:
        return (None, None)

    inlier_coordinates = guide_location_inliers(coordinates)
    lat = sum(latitude for latitude, _longitude in inlier_coordinates) / len(inlier_coordinates)
    lng = sum(longitude for _latitude, longitude in inlier_coordinates) / len(inlier_coordinates)
    return (lat, lng)


def warn_far_map_pins(
    slug: str,
    places: list[NormalizedPlace],
    center: tuple[float | None, float | None],
) -> None:
    warning_distance_meters = guide_map_pin_warning_distance_meters(places, center)
    center_lat, center_lng = center
    if center_lat is None or center_lng is None or warning_distance_meters is None:
        return

    for place in places:
        if place.lat is None or place.lng is None:
            continue

        distance_meters = haversine_meters(center_lat, center_lng, place.lat, place.lng)
        if distance_meters < warning_distance_meters:
            continue

        print(
            "WARNING: "
            f"{slug}:{place.id} map pin for {place.name!r} is "
            f"{distance_meters / 1000:.0f} km from the guide center; "
            "check whether it belongs in this city/country."
        )


def guide_map_pin_warning_distance_meters(
    places: list[NormalizedPlace],
    center: tuple[float | None, float | None],
) -> float | None:
    center_lat, center_lng = center
    if center_lat is None or center_lng is None:
        return None

    coordinates = [
        (place.lat, place.lng)
        for place in places
        if place.lat is not None and place.lng is not None
    ]
    if not coordinates:
        return None
    if len(coordinates) < 4:
        return MAP_PIN_DISTANCE_WARNING_MIN_METERS

    inlier_coordinates = guide_location_inliers(coordinates)
    inlier_radius_meters = max(
        haversine_meters(center_lat, center_lng, latitude, longitude)
        for latitude, longitude in inlier_coordinates
    )
    return max(
        MAP_PIN_DISTANCE_WARNING_MIN_METERS,
        inlier_radius_meters + MAP_PIN_DISTANCE_WARNING_BUFFER_METERS,
    )


def guide_location_inliers(coordinates: list[tuple[float, float]]) -> list[tuple[float, float]]:
    if len(coordinates) < 4:
        return coordinates

    medoid_lat, medoid_lng = min(
        coordinates,
        key=lambda coordinate: sum(
            haversine_meters(coordinate[0], coordinate[1], latitude, longitude)
            for latitude, longitude in coordinates
        ),
    )
    distances = [
        haversine_meters(medoid_lat, medoid_lng, latitude, longitude)
        for latitude, longitude in coordinates
    ]
    median_distance = median(distances)
    median_absolute_deviation = median(abs(distance - median_distance) for distance in distances)
    outlier_threshold = max(50_000.0, median_distance + 6 * median_absolute_deviation)

    inliers = [
        coordinate
        for coordinate, distance in zip(coordinates, distances, strict=True)
        if distance <= outlier_threshold
    ]
    return inliers or coordinates


def percentile(sorted_values: list[float], fraction: float) -> float:
    if not sorted_values:
        raise ValueError("Cannot calculate percentile for an empty list.")

    position = (len(sorted_values) - 1) * fraction
    lower_index = math.floor(position)
    upper_index = math.ceil(position)
    if lower_index == upper_index:
        return sorted_values[lower_index]

    lower_value = sorted_values[lower_index]
    upper_value = sorted_values[upper_index]
    return lower_value + (upper_value - lower_value) * (position - lower_index)


def enrich_raw_sources(*, force_refresh: bool) -> None:
    api_key = google_places_api_key()

    for raw_path in sorted(RAW_DIR.glob("*.json")):
        raw = RawSavedList.model_validate_json(raw_path.read_text(encoding="utf-8"))
        cache_payload = load_places_cache(raw_path.stem)
        changed = False
        for place in raw.places:
            place_id = stable_place_id(place, source_type=raw.configured_source_type)
            cache_entry = cache_payload.get(place_id)
            refresh_reason = None if force_refresh else cache_refresh_reason(place, cache_entry)
            if not force_refresh and refresh_reason is None:
                continue

            if force_refresh:
                print(f"Enriching {raw_path.stem}:{place_id} (forced)")
            else:
                print(f"Enriching {raw_path.stem}:{place_id} ({refresh_reason})")
            cache_payload[place_id] = fetch_places_enrichment(place, api_key=api_key)
            changed = True

        if changed:
            save_places_cache(raw_path.stem, cache_payload)


def stable_place_id(place: RawPlace, *, source_type: str | None = None) -> str:
    cid = place.cid
    if cid:
        return f"cid:{cid}"

    google_id = place.google_id
    if google_id:
        normalized = google_id.strip("/").replace("/", "-")
        return f"gid:{normalized}"

    cid_from_url = extract_maps_cid(place.maps_url)
    if cid_from_url:
        return f"cid:{cid_from_url}"

    maps_place_token = place.maps_place_token or extract_maps_place_token(place.maps_url)
    if maps_place_token:
        return f"gms:{maps_place_token}"

    if source_type == "google_export_csv":
        maps_url_id = short_maps_url_id(place.maps_url)
        if maps_url_id:
            return maps_url_id

    fallback = f"{place.name or 'place'}-{place.lat}-{place.lng}"
    return f"slug:{slugify(fallback)}"


def short_maps_url_id(maps_url: str | None) -> str | None:
    normalized_url = as_string(maps_url)
    if normalized_url is None:
        return None
    digest = hashlib.sha256(normalized_url.encode("utf-8")).hexdigest()[:16]
    return f"url:{digest}"


def derive_place_tags(
    place: RawPlace,
    city_name: str | None,
    *,
    enrichment: EnrichmentPlace,
    category: str | None,
) -> list[str]:
    tags: set[str] = set()
    if city_name:
        tags.add(slugify(city_name))
    for locality in infer_address_localities(place.address, city_name=city_name):
        tags.add(slugify(locality))
    if category:
        tags.add(slugify(category))
    for place_type in enrichment.types[:4]:
        tags.add(slugify(place_type.replace("_", "-")))
    return sorted(tag for tag in tags if tag)


def derive_marker_icon(
    place: RawPlace,
    *,
    enrichment: EnrichmentPlace,
    category: str | None,
    tags: list[str],
    note: str | None,
    why_recommended: str | None,
) -> str:
    candidate_slugs = [
        slugify(term.replace("_", "-"))
        for term in [
            category,
            enrichment.primary_type,
            enrichment.primary_type_display_name,
            *enrichment.types,
            *tags,
        ]
        if term
    ]

    for candidate_slug in candidate_slugs:
        for marker_icon, phrases in MARKER_ICON_RULES:
            if any(slug_phrase_matches(candidate_slug, phrase) for phrase in phrases):
                return marker_icon

    lookup_text = normalize_vibe_lookup_text(
        " ".join(
            filter(
                None,
                [
                    place.name,
                    place.address,
                    place.note,
                    note,
                    why_recommended,
                    category,
                    " ".join(tags),
                ],
            )
        )
    )
    for marker_icon, keywords in MARKER_ICON_TEXT_RULES:
        if any(vibe_keyword_matches(lookup_text, keyword) for keyword in keywords):
            return marker_icon

    return "default"


def derive_vibe_tags(
    place: RawPlace,
    *,
    enrichment: EnrichmentPlace,
    category: str | None,
    tags: list[str],
    note: str | None,
    why_recommended: str | None,
    top_pick: bool,
) -> list[str]:
    vibes: set[str] = set()
    category_terms = [
        category,
        enrichment.primary_type,
        enrichment.primary_type_display_name,
        *enrichment.types,
        *tags,
    ]
    category_slugs = {slugify(term.replace("_", "-")) for term in category_terms if term}

    for category_slug in category_slugs:
        if category_slug in VIBE_CATEGORY_RULES:
            vibes.update(VIBE_CATEGORY_RULES[category_slug])

    lookup_text = normalize_vibe_lookup_text(
        " ".join(
            filter(
                None,
                [
                    place.name,
                    place.address,
                    place.note,
                    note,
                    why_recommended,
                    category,
                    enrichment.primary_type,
                    enrichment.primary_type_display_name,
                    " ".join(enrichment.types),
                    " ".join(tags),
                ],
            )
        )
    )
    for vibe_tag, keywords in VIBE_TAG_KEYWORDS.items():
        if any(vibe_keyword_matches(lookup_text, keyword) for keyword in keywords):
            vibes.add(vibe_tag)

    if top_pick:
        vibes.add("local-favorite")

    return sorted(vibes)


def normalize_vibe_lookup_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("_", " ").replace("-", " ").lower()).strip()


def slug_phrase_matches(candidate_slug: str, phrase: str) -> bool:
    normalized_phrase = slugify(phrase.replace("_", "-"))
    if not candidate_slug or not normalized_phrase:
        return False
    return (
        candidate_slug == normalized_phrase
        or candidate_slug.startswith(f"{normalized_phrase}-")
        or candidate_slug.endswith(f"-{normalized_phrase}")
        or f"-{normalized_phrase}-" in candidate_slug
    )


def vibe_keyword_matches(lookup_text: str, keyword: str) -> bool:
    normalized_keyword = normalize_vibe_lookup_text(keyword)
    if not normalized_keyword:
        return False
    pattern = r"(?<![a-z0-9])" + re.escape(normalized_keyword).replace(r"\ ", r"\s+") + r"(?![a-z0-9])"
    return re.search(pattern, lookup_text) is not None


def build_search_index(guides: list[Guide]) -> dict[str, Any]:
    generated_at = datetime.now(UTC).isoformat()
    return {
        "version": 1,
        "generated_at": generated_at,
        "guides": [search_index_guide_entry(guide) for guide in guides],
        "entries": [
            search_index_place_entry(guide, place)
            for guide in guides
            for place in guide.places
            if not place.hidden
        ],
    }


def search_index_guide_entry(guide: Guide) -> dict[str, Any]:
    featured_ids = set(guide.featured_place_ids)
    return {
        "slug": guide.slug,
        "title": guide.title,
        "description": guide.description,
        "city": guide.city_name,
        "country": guide.country_name,
        "country_code": guide.country_code,
        "tags": guide.list_tags,
        "place_count": guide.place_count,
        "top_categories": guide.top_categories,
        "featured_names": [
            place.name
            for place in guide.places
            if place.id in featured_ids and not place.hidden
        ][:3],
        "url": f"/guides/{guide.slug}/",
        "search_text": compact_search_text(
            [
                guide.title,
                guide.description,
                guide.city_name,
                guide.country_name,
                guide.country_code,
                " ".join(guide.list_tags),
                " ".join(guide.top_categories),
            ]
        ),
    }


def search_index_place_entry(guide: Guide, place: NormalizedPlace) -> dict[str, Any]:
    return {
        "id": place.id,
        "guide_slug": guide.slug,
        "guide_title": guide.title,
        "city": guide.city_name,
        "country": guide.country_name,
        "country_code": guide.country_code,
        "name": place.name,
        "category": place.primary_category,
        "neighborhood": place.neighborhood,
        "tags": place.tags,
        "vibe_tags": place.vibe_tags,
        "note": place.note,
        "why_recommended": place.why_recommended,
        "top_pick": place.top_pick,
        "manual_rank": place.manual_rank,
        "maps_url": place.maps_url,
        "url": f"/guides/{guide.slug}/?place={quote_query_value(place.id)}",
        "search_text": compact_search_text(
            [
                place.name,
                place.address,
                place.note,
                place.why_recommended,
                place.primary_category,
                place.neighborhood,
                " ".join(place.tags),
                " ".join(place.vibe_tags),
                guide.title,
                guide.city_name,
                guide.country_name,
                guide.country_code,
            ]
        ),
    }


def compact_search_text(parts: list[str | None]) -> str:
    return re.sub(r"\s+", " ", " ".join(part for part in parts if part).lower()).strip()


def quote_query_value(value: str) -> str:
    from urllib.parse import quote

    return quote(value, safe="")


def infer_city_name(title: str) -> str | None:
    parts = split_title_parts(title)
    if not parts:
        return None
    return parts[0]


def infer_country_name(title: str, raw: RawSavedList) -> str | None:
    parts = split_title_parts(title)
    if len(parts) > 1:
        return parts[-1]
    if len(parts) == 1 and known_country_name(parts[0]):
        return parts[0]
    flag_country_name = infer_country_from_flag(title) or infer_country_from_flag(raw.description)
    if flag_country_name:
        return flag_country_name

    place_countries = [infer_country_from_address(place.address) for place in raw.places]
    place_countries = [country for country in place_countries if country]
    if not place_countries:
        return None
    return Counter(place_countries).most_common(1)[0][0]


def infer_country_code(country_name: str | None) -> str | None:
    if country_name is None:
        return None
    mapping = {
        "Japan": "JP",
        "South Korea": "KR",
        "Korea": "KR",
        "Taiwan": "TW",
        "United States": "US",
    }
    mapped_code = mapping.get(country_name)
    if mapped_code:
        return mapped_code
    try:
        country = pycountry.countries.lookup(country_name)
    except LookupError:
        return None
    return country.alpha_2


def infer_country_from_address(address: str | None) -> str | None:
    if address is None:
        return None
    parts = [part.strip() for part in address.split(",") if part.strip()]
    if not parts:
        return None

    for part in reversed(parts):
        country_name = known_country_name(normalize_country_candidate(part))
        if country_name:
            return country_name

    return None


def normalize_country_candidate(value: str) -> str:
    candidate = re.sub(r"[^\w\s-]", " ", value).strip()
    candidate = re.sub(r"\b\d{3,6}(?:[-−ー－]\d{4})?\b", " ", candidate)
    candidate = re.sub(r"\s+", " ", candidate).strip(" -−ー－")
    return candidate


def known_country_name(candidate: str | None) -> str | None:
    if candidate is None:
        return None
    candidate = candidate.strip()
    if not candidate:
        return None

    aliases = {
        "england": "England",
        "scotland": "Scotland",
        "wales": "Wales",
        "northern ireland": "Northern Ireland",
        "uk": "UK",
        "uae": "UAE",
        "usa": "USA",
        "korea": "Korea",
        "taiwan": "Taiwan",
        "vatican city": "Vatican City",
        "ivory coast": "Ivory Coast",
    }
    alias = aliases.get(normalize_locality_key(candidate))
    if alias:
        return alias

    try:
        pycountry.countries.lookup(candidate)
    except LookupError:
        return None
    return candidate


def infer_country_from_flag(text: str | None) -> str | None:
    if text is None:
        return None

    flag_country_names = {
        "AE": "United Arab Emirates",
        "GB": "United Kingdom",
        "KR": "South Korea",
        "TW": "Taiwan",
        "US": "United States",
    }
    regional_indicator_start = 0x1F1E6
    regional_indicator_end = 0x1F1FF
    regional_indicators = [
        char for char in text if regional_indicator_start <= ord(char) <= regional_indicator_end
    ]
    for index in range(len(regional_indicators) - 1):
        first = ord(regional_indicators[index]) - regional_indicator_start
        second = ord(regional_indicators[index + 1]) - regional_indicator_start
        country_code = f"{chr(ord('A') + first)}{chr(ord('A') + second)}"
        mapped_name = flag_country_names.get(country_code)
        if mapped_name:
            return mapped_name
        country = pycountry.countries.get(alpha_2=country_code)
        if country:
            return country.name
    return None


def infer_neighborhood(address: str | None, *, city_name: str | None = None) -> str | None:
    localities = infer_address_localities(address, city_name=city_name)
    if localities:
        return localities[0]
    return None


def infer_address_localities(address: str | None, *, city_name: str | None = None) -> list[str]:
    if address is None:
        return []

    city_key = normalize_locality_key(city_name)
    neighborhoods: list[str] = []
    subcities: list[str] = []

    for raw_part in re.split(r"[,、]", address):
        candidate = normalize_address_locality_part(raw_part)
        if candidate is None:
            continue
        key = normalize_locality_key(candidate)
        if not key or key == city_key:
            continue
        if is_subcity_locality(candidate):
            append_unique_locality(subcities, candidate)
        else:
            append_unique_locality(neighborhoods, candidate)

    return [*neighborhoods, *subcities]


def normalize_address_locality_part(part: str) -> str | None:
    candidate = part.strip()
    if not candidate:
        return None

    candidate = re.sub(r"^(?:japan|日本)\s*", "", candidate, flags=re.IGNORECASE).strip()
    candidate = re.sub(r"〒\s*\d{3}[-−ー－]?\d{4}\s*", "", candidate).strip()
    candidate = re.sub(r"\b\d{3}[-−ー－]\d{4}\b", "", candidate).strip()
    candidate = re.sub(r"\b\d{5}(?:-\d{4})?\b", "", candidate).strip()
    candidate = re.sub(r"\s+\d{4}\b", "", candidate).strip()
    candidate = candidate.strip(" -−ー－/()[]{}.")

    if not candidate or is_country_locality(candidate):
        return None

    if is_building_or_unit_part(candidate):
        return None

    trailing_locality = extract_trailing_locality(candidate)
    if trailing_locality is not None:
        candidate = trailing_locality
    else:
        candidate = re.sub(r"^\d+[A-Za-z]?(?:[-−ー－/]\d+[A-Za-z]?)*\s+", "", candidate).strip()

    if is_street_or_block_part(candidate) or is_building_or_unit_part(candidate):
        return None

    candidate = re.sub(r"\s+", " ", candidate).strip(" -−ー－/()[]{}.")
    if not candidate:
        return None
    if re.search(r"\d", candidate):
        return None
    if len(candidate) <= 1:
        return None
    return candidate


def extract_trailing_locality(candidate: str) -> str | None:
    patterns = [
        r"^\d+\s*-?\s*chome(?:[-−ー－]\d+)*\s+([A-Za-z][A-Za-z' -]+)$",
        r"^\d+\s*丁目(?:[-−ー－]\d+)*\s+([A-Za-z][A-Za-z' -]+)$",
        r"^(?:\d+[Ff]?\s*,?\s*)?(?:\d+[-−ー－/])+\d+\s+([A-Za-z][A-Za-z' -]+)$",
    ]
    for pattern in patterns:
        match = re.search(pattern, candidate, flags=re.IGNORECASE)
        if match:
            locality = match.group(1).strip(" -−ー－/()[]{}.")
            if locality and not is_building_or_unit_part(locality):
                return locality
    return None


def is_country_locality(candidate: str) -> bool:
    return normalize_locality_key(candidate) in get_country_locality_keys()


def get_country_locality_keys() -> set[str]:
    global COUNTRY_LOCALITY_KEYS
    if COUNTRY_LOCALITY_KEYS is not None:
        return COUNTRY_LOCALITY_KEYS

    country_names = set(COUNTRY_LOCALITY_ALIASES)
    for country in pycountry.countries:
        for attribute in ("name", "official_name", "common_name"):
            value = getattr(country, attribute, None)
            if value:
                country_names.add(value)

    COUNTRY_LOCALITY_KEYS = {normalize_locality_key(name) for name in country_names}
    return COUNTRY_LOCALITY_KEYS


def is_subcity_locality(candidate: str) -> bool:
    return bool(
        re.search(
            r"\b(?:city|ward|district|borough|county|prefecture|province|gu|ku)\b",
            candidate,
            flags=re.IGNORECASE,
        )
    )


def is_building_or_unit_part(candidate: str) -> bool:
    return bool(
        re.search(
            r"\b(?:bldg|building|tower|plaza|terrace|floor|mall|hotel|mansion|palace|garden|stream|works|center|centre|place|v-city|gratteciel|gems)\b|ビル|階|号|館",
            candidate,
            flags=re.IGNORECASE,
        )
    )


def is_street_or_block_part(candidate: str) -> bool:
    return bool(
        re.search(
            r"\b(?:chome|丁目|st|street|rd|road|ln|lane|ave|avenue|dr|drive|blvd|boulevard|rue|via)\b",
            candidate,
            flags=re.IGNORECASE,
        )
    )


def append_unique_locality(localities: list[str], candidate: str) -> None:
    key = normalize_locality_key(candidate)
    if key and all(normalize_locality_key(existing) != key for existing in localities):
        localities.append(candidate)


def normalize_locality_key(value: str | None) -> str:
    if value is None:
        return ""
    cleaned = re.sub(r"[^\w\s-]", " ", value.strip().lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def split_title_parts(title: str) -> list[str]:
    cleaned = re.sub(r"[^\w\s,&/-]", "", title)
    return [part.strip() for part in cleaned.split(",") if part.strip()]


def extract_hashtags(text: str | None) -> list[str]:
    if text is None:
        return []
    return sorted({match.group(1).lower() for match in re.finditer(r"#([a-zA-Z0-9-]+)", text)})


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.lower()).strip("-")
    return slug or "item"


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def load_raw_saved_list(path: Path) -> RawSavedList | None:
    if not path.exists():
        return None
    return RawSavedList.model_validate_json(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any, *, compact: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if hasattr(payload, "model_dump"):
        payload = payload.model_dump(mode="json")
    elif isinstance(payload, list):
        payload = [item.model_dump(mode="json") if hasattr(item, "model_dump") else item for item in payload]
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    if compact:
        text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    else:
        text = json.dumps(payload, indent=2, ensure_ascii=False)
    tmp_path.write_text(f"{text}\n", encoding="utf-8")
    tmp_path.replace(path)


def as_string(value: Any) -> str | None:
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return None


def as_float(value: Any) -> float | None:
    if isinstance(value, int | float):
        return float(value)
    return None


def as_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    return None


def as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    return None


def coerce_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]


def google_places_api_key() -> str | None:
    return PlacesSettings().google_places_api_key


def configured_source_path(source: SourceConfig) -> Path:
    if not source.path:
        raise RuntimeError(f"Configured source {source.slug} is missing a path.")
    path = Path(source.path).expanduser()
    if not path.is_absolute():
        path = ROOT / path
    return path


def fallback_source_title(source: SourceConfig) -> str:
    if source.title:
        return source.title
    if source.path:
        stem = Path(source.path).stem
    elif source.url:
        stem = source.slug
    else:
        stem = "saved-list"
    return stem.replace("-", " ").replace("_", " ").title()


def raw_source_signature(source: SourceConfig) -> str:
    payload = {
        "slug": source.slug,
        "type": source.type,
        "url": source.url,
        "path": source.path,
        "title": source.title,
    }
    if source.type == "google_export_csv":
        path = configured_source_path(source)
        payload["content_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def stamp_raw_saved_list(payload: RawSavedList, source: SourceConfig, *, source_signature: str) -> None:
    fetched_at = datetime.now(UTC)
    payload.fetched_at = fetched_at.isoformat()
    payload.refresh_after = raw_source_refresh_after(
        fetched_at,
        source,
        source_signature=source_signature,
    ).isoformat()
    payload.source_signature = source_signature
    payload.configured_source_type = source.type
    payload.configured_source_url = source.url
    payload.configured_source_path = source.path


def raw_source_refresh_after(fetched_at: datetime, source: SourceConfig, *, source_signature: str) -> datetime:
    if source.type != "google_list_url":
        return fetched_at + RAW_SOURCE_CACHE_TTL
    return fetched_at + RAW_SOURCE_CACHE_TTL + stable_timedelta_jitter(
        source_signature,
        max_jitter=RAW_SOURCE_REFRESH_JITTER,
    )


def stable_timedelta_jitter(key: str, *, max_jitter: timedelta) -> timedelta:
    max_seconds = int(max_jitter.total_seconds())
    if max_seconds <= 0:
        return timedelta()

    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    offset_seconds = int(digest[:12], 16) % (max_seconds * 2 + 1)
    return timedelta(seconds=offset_seconds - max_seconds)


def raw_source_refresh_reason(source: SourceConfig, existing_payload: RawSavedList | None) -> str | None:
    if existing_payload is None:
        return "missing-raw-snapshot"

    if not raw_source_signature_matches(source, existing_payload.source_signature):
        return "source-config-changed"

    if existing_payload.refresh_after:
        try:
            refresh_after_dt = parse_metadata_datetime(existing_payload.refresh_after)
        except ValueError:
            return "invalid-refresh-after"
        if datetime.now(UTC) >= refresh_after_dt:
            return "refresh-window-expired"
        return None

    if existing_payload.fetched_at:
        try:
            fetched_at_dt = parse_metadata_datetime(existing_payload.fetched_at)
        except ValueError:
            return "invalid-fetched-at"
        if datetime.now(UTC) - fetched_at_dt >= RAW_SOURCE_CACHE_TTL:
            return "legacy-refresh-window-expired"
        return None

    return "missing-refresh-metadata"


def parse_metadata_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def raw_source_signature_matches(source: SourceConfig, signature: str | None) -> bool:
    return signature in raw_source_signature_candidates(source)


def raw_source_signature_candidates(source: SourceConfig) -> set[str]:
    signatures = {raw_source_signature(source)}
    if source.type == "google_list_url":
        signatures.add(legacy_google_list_source_signature(source))
    return signatures


def legacy_google_list_source_signature(source: SourceConfig) -> str:
    payload = {
        "slug": source.slug,
        "url": source.url,
        "title": source.title,
        "refresh_days": RAW_SOURCE_CACHE_TTL.days,
    }
    serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def resolve_refresh_sources(sources: list[SourceConfig], selectors: list[str]) -> list[SourceConfig]:
    if not selectors:
        return []

    normalized_selectors = {selector.strip() for selector in selectors if selector.strip()}
    matched_sources: list[SourceConfig] = []
    missing_selectors = set(normalized_selectors)

    for source in sources:
        if (
            source.slug in normalized_selectors
            or source.url in normalized_selectors
            or source.path in normalized_selectors
        ):
            matched_sources.append(source)
            missing_selectors.discard(source.slug)
            missing_selectors.discard(source.url)
            missing_selectors.discard(source.path)

    if missing_selectors:
        missing_text = ", ".join(sorted(missing_selectors))
        raise RuntimeError(f"No configured list matched: {missing_text}")

    return matched_sources


def cache_refresh_reason(place: RawPlace, cache_entry: EnrichmentCacheEntry | None) -> str | None:
    if cache_entry is None:
        return "missing-cache-entry"

    expected_signature = place_input_signature(place)
    if cache_entry.input_signature != expected_signature:
        return "raw-place-changed"

    if cache_entry.refresh_after:
        try:
            refresh_after_dt = datetime.fromisoformat(cache_entry.refresh_after)
        except ValueError:
            return "invalid-refresh-after"
        if datetime.now(UTC) >= refresh_after_dt:
            return "refresh-window-expired"
        return None

    try:
        fetched_at_dt = datetime.fromisoformat(cache_entry.fetched_at)
    except ValueError:
        return "invalid-fetched-at"
    if datetime.now(UTC) - fetched_at_dt > OPERATIONAL_CACHE_TTL:
        return "legacy-cache-entry"
    if cache_entry.input_signature is None:
        return "missing-input-signature"
    return None


def place_input_signature(place: RawPlace) -> str:
    payload = {
        "name": place.name,
        "address": place.address,
        "lat": place.lat,
        "lng": place.lng,
        "maps_url": place.maps_url,
        "cid": place.cid,
        "google_id": place.google_id,
        "maps_place_token": place.maps_place_token,
    }
    serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def cache_refresh_ttl(cache_entry: EnrichmentCacheEntry) -> timedelta:
    if cache_entry.error:
        return ERROR_CACHE_TTL
    if cache_entry.matched is False or cache_entry.place is None:
        return UNMATCHED_CACHE_TTL
    if cache_entry.score is None or cache_entry.score < STRONG_MATCH_SCORE:
        return LOW_CONFIDENCE_CACHE_TTL

    place = cache_entry.place
    if place.business_status and place.business_status != "OPERATIONAL":
        return NON_OPERATIONAL_CACHE_TTL
    if place.rating is not None or place.user_rating_count is not None:
        return RATINGS_CACHE_TTL
    return OPERATIONAL_CACHE_TTL


def build_cache_entry(
    place: RawPlace,
    *,
    source: str | None = None,
    query: str,
    matched: bool | None = None,
    score: int | None = None,
    error: str | None = None,
    error_body: str | None = None,
    enrichment_place: EnrichmentPlace | None = None,
) -> EnrichmentCacheEntry:
    fetched_at = datetime.now(UTC)
    cache_entry = EnrichmentCacheEntry(
        fetched_at=fetched_at.isoformat(),
        last_verified_at=fetched_at.isoformat(),
        source=source,
        query=query,
        input_signature=place_input_signature(place),
        matched=matched,
        score=score,
        error=error,
        error_body=error_body,
        place=enrichment_place,
    )
    cache_entry.refresh_after = (fetched_at + cache_refresh_ttl(cache_entry)).isoformat()
    return cache_entry


def load_places_cache(slug: str) -> dict[str, EnrichmentCacheEntry]:
    payload = read_json(PLACES_CACHE_DIR / f"{slug}.json")
    result: dict[str, EnrichmentCacheEntry] = {}
    for place_id, entry in payload.items():
        if isinstance(place_id, str) and isinstance(entry, dict):
            result[place_id] = EnrichmentCacheEntry.model_validate(entry)
    return result


def save_places_cache(slug: str, payload: dict[str, EnrichmentCacheEntry]) -> None:
    write_json(PLACES_CACHE_DIR / f"{slug}.json", payload)


def fetch_places_enrichment(place: RawPlace, *, api_key: str | None) -> EnrichmentCacheEntry:
    page_entry = fetch_place_page_enrichment(place)
    if page_entry.error is None and page_entry.place is not None:
        if api_key is None or not should_fallback_to_places_api(page_entry):
            return page_entry

    if api_key is not None:
        api_entry = fetch_places_api_enrichment(place, api_key=api_key)
        if (
            page_entry.error is None
            and page_entry.place is not None
            and (api_entry.error is not None or api_entry.matched is not True or api_entry.place is None)
        ):
            return page_entry
        return api_entry

    return page_entry


def fetch_place_page_enrichment(place: RawPlace) -> EnrichmentCacheEntry:
    query = build_text_query(place)
    if scrape_place is None:
        return build_cache_entry(
            place,
            source="google_maps_page",
            query=query,
            error="gmaps_scraper_unavailable",
        )

    proxy = current_scraper_proxy()
    session_state, browser_session, http_session = build_scraper_sessions(proxy)
    try:
        try:
            details = scrape_place(
                place.maps_url,
                headless=True,
                browser_session=browser_session,
                http_session=http_session,
            )
        except RECOVERABLE_REFRESH_ERRORS as exc:
            if should_reset_scraper_session(exc):
                clear_scraper_session_state(session_state)
                browser_session, http_session = build_scraper_configs(session_state, proxy)
                try:
                    details = scrape_place(
                        place.maps_url,
                        headless=True,
                        browser_session=browser_session,
                        http_session=http_session,
                    )
                except RECOVERABLE_REFRESH_ERRORS as retry_exc:
                    return build_cache_entry(
                        place,
                        source="google_maps_page",
                        query=query,
                        error=f"scrape_error:{retry_exc}",
                    )
            else:
                return build_cache_entry(
                    place,
                    source="google_maps_page",
                    query=query,
                    error=f"scrape_error:{exc}",
                )

        record_scraper_session_use(session_state, proxy=proxy)
        enrichment_place = normalize_place_page_enrichment(details)
        matched = place_page_has_meaningful_enrichment(details, enrichment_place)
        return build_cache_entry(
            place,
            source="google_maps_page",
            query=query,
            matched=matched,
            score=STRONG_MATCH_SCORE if matched else None,
            enrichment_place=enrichment_place if matched else None,
        )
    finally:
        release_scraper_session_lock(session_state)


def should_fallback_to_places_api(entry: EnrichmentCacheEntry) -> bool:
    place = entry.place
    if place is None:
        return True
    if entry.source != "google_maps_page":
        return False
    if place.limited_view:
        return True
    if place.business_status is None and place.rating is None and place.user_rating_count is None:
        if not place.primary_type_display_name:
            return True
    return False


def place_page_has_meaningful_enrichment(
    details: Any,
    enrichment_place: EnrichmentPlace,
) -> bool:
    if (
        enrichment_place.display_name
        or enrichment_place.formatted_address
        or enrichment_place.primary_type_display_name
        or enrichment_place.rating is not None
        or enrichment_place.user_rating_count is not None
    ):
        return True
    return not bool(getattr(details, "limited_view", False)) and bool(
        enrichment_place.website
        or enrichment_place.phone
        or enrichment_place.plus_code
        or enrichment_place.description
    )


def normalize_place_page_enrichment(details: Any) -> EnrichmentPlace:
    category = as_string(getattr(details, "category", None))
    primary_type = None
    types: list[str] = []
    if category is not None:
        primary_type = slugify(category).replace("-", "_")
        if primary_type:
            types.append(primary_type)

    display_name = as_string(getattr(details, "name", None))
    formatted_address = as_string(getattr(details, "address", None))
    rating = as_float(getattr(details, "rating", None))
    user_rating_count = as_int(getattr(details, "review_count", None))
    website = as_string(getattr(details, "website", None))
    phone = as_string(getattr(details, "phone", None))
    plus_code = as_string(getattr(details, "plus_code", None))
    description = as_string(getattr(details, "description", None))
    limited_view = bool(getattr(details, "limited_view", False))
    has_meaningful_fields = any(
        (
            display_name,
            formatted_address,
            primary_type,
            category,
            rating is not None,
            user_rating_count is not None,
            website,
            phone,
            plus_code,
            description,
        )
    )
    maps_uri = None
    if limited_view or has_meaningful_fields:
        maps_uri = as_string(getattr(details, "resolved_url", None)) or as_string(
            getattr(details, "source_url", None)
        )
    return EnrichmentPlace(
        display_name=display_name,
        formatted_address=formatted_address,
        google_maps_uri=maps_uri,
        rating=rating,
        user_rating_count=user_rating_count,
        primary_type=primary_type,
        primary_type_display_name=category,
        types=types,
        business_status=normalize_place_page_business_status(as_string(getattr(details, "status", None))),
        website=website,
        phone=phone,
        plus_code=plus_code,
        description=description,
        limited_view=limited_view,
    )


def normalize_place_page_business_status(status: str | None) -> str | None:
    normalized = as_string(status)
    if normalized is None:
        return None
    lowered = normalized.lower()
    if "permanently closed" in lowered:
        return "CLOSED_PERMANENTLY"
    if "temporarily closed" in lowered:
        return "CLOSED_TEMPORARILY"
    if lowered.startswith("open") or lowered.startswith("closed") or "opens " in lowered:
        return "OPERATIONAL"
    return None


def fetch_places_api_enrichment(place: RawPlace, *, api_key: str) -> EnrichmentCacheEntry:
    query = build_text_query(place)
    request_body = {
        "textQuery": query,
        "pageSize": 3,
        "languageCode": "en",
    }

    lat = place.lat
    lng = place.lng
    if lat is not None and lng is not None:
        request_body["locationBias"] = {
            "circle": {
                "center": {"latitude": lat, "longitude": lng},
                "radius": 600.0,
            }
        }

    request = Request(
        PLACES_TEXT_SEARCH_URL,
        data=json.dumps(request_body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-Goog-Api-Key": api_key,
            "X-Goog-FieldMask": PLACES_FIELD_MASK,
        },
        method="POST",
    )

    try:
        with urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return build_cache_entry(
            place,
            source="google_places_api",
            query=query,
            error=f"http_{exc.code}",
            error_body=body[:500],
        )
    except URLError as exc:
        return build_cache_entry(
            place,
            source="google_places_api",
            query=query,
            error=f"url_error:{exc.reason}",
        )

    matches = payload.get("places")
    if not isinstance(matches, list) or not matches:
        return build_cache_entry(
            place,
            source="google_places_api",
            query=query,
            matched=False,
        )

    scored_matches = [
        (score_text_search_candidate(place, candidate), candidate)
        for candidate in matches
        if isinstance(candidate, dict)
    ]
    scored_matches.sort(key=lambda item: item[0], reverse=True)
    best_score, best_match = scored_matches[0]

    result = build_cache_entry(
        place,
        source="google_places_api",
        query=query,
        matched=best_score >= 25,
        score=best_score,
        enrichment_place=normalize_enrichment_match(best_match) if best_score >= 25 else None,
    )
    return result


def build_text_query(place: RawPlace) -> str:
    name = place.name
    address = place.address or ""
    query = f"{name}, {address}".strip(", ")
    return query or name or address


def score_text_search_candidate(raw_place: RawPlace, candidate: dict[str, Any]) -> int:
    score = 0
    raw_name = normalize_text(raw_place.name)
    candidate_name = normalize_text(display_name_text(candidate.get("displayName")))
    raw_address = normalize_text(raw_place.address)
    candidate_address = normalize_text(as_string(candidate.get("formattedAddress")))

    if raw_name and candidate_name:
        if raw_name == candidate_name:
            score += 50
        elif raw_name in candidate_name or candidate_name in raw_name:
            score += 30
        else:
            overlap = token_overlap_score(raw_name, candidate_name)
            score += overlap

    if raw_address and candidate_address:
        score += token_overlap_score(raw_address, candidate_address)

    raw_lat = raw_place.lat
    raw_lng = raw_place.lng
    candidate_location = candidate.get("location")
    if (
        raw_lat is not None
        and raw_lng is not None
        and isinstance(candidate_location, dict)
        and as_float(candidate_location.get("latitude")) is not None
        and as_float(candidate_location.get("longitude")) is not None
    ):
        distance_m = haversine_meters(
            raw_lat,
            raw_lng,
            as_float(candidate_location.get("latitude")) or raw_lat,
            as_float(candidate_location.get("longitude")) or raw_lng,
        )
        if distance_m <= 100:
            score += 40
        elif distance_m <= 400:
            score += 25
        elif distance_m <= 1200:
            score += 10

    return score


def normalize_enrichment_match(candidate: dict[str, Any]) -> EnrichmentPlace:
    display_name = candidate.get("displayName")
    primary_type_display_name = candidate.get("primaryTypeDisplayName")
    return EnrichmentPlace(
        google_place_id=as_string(candidate.get("id")),
        google_place_resource_name=as_string(candidate.get("name")),
        display_name=display_name_text(display_name),
        formatted_address=as_string(candidate.get("formattedAddress")),
        google_maps_uri=as_string(candidate.get("googleMapsUri")),
        rating=as_float(candidate.get("rating")),
        user_rating_count=as_int(candidate.get("userRatingCount")),
        primary_type=as_string(candidate.get("primaryType")),
        primary_type_display_name=display_name_text(primary_type_display_name),
        types=coerce_string_list(candidate.get("types")),
        business_status=as_string(candidate.get("businessStatus")),
    )


def coerce_enrichment_place(value: EnrichmentCacheEntry | None) -> EnrichmentPlace:
    if value is None or value.place is None:
        return EnrichmentPlace()
    return value.place


def display_name_text(value: Any) -> str | None:
    if isinstance(value, dict):
        return as_string(value.get("text"))
    if isinstance(value, str):
        return as_string(value)
    return None


def normalize_business_status(value: str | None) -> str | None:
    mapping = {
        "OPERATIONAL": "active",
        "CLOSED_TEMPORARILY": "temporarily-closed",
        "CLOSED_PERMANENTLY": "closed-permanently",
        "FUTURE_OPENING": "future-opening",
    }
    if value is None:
        return None
    return mapping.get(value, slugify(value))


def normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    return re.sub(r"[^a-z0-9\s]", " ", value.lower()).strip()


def token_overlap_score(left: str, right: str) -> int:
    left_tokens = {token for token in left.split() if len(token) > 2}
    right_tokens = {token for token in right.split() if len(token) > 2}
    if not left_tokens or not right_tokens:
        return 0
    overlap = len(left_tokens & right_tokens)
    return overlap * 5


def haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6_371_000
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius * c


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if (args.refresh_list or args.refresh_force) and not args.refresh:
        args.refresh = True

    if args.refresh:
        refresh_raw_sources(
            headed=args.headed,
            force_refresh=args.refresh_force,
            refresh_lists=args.refresh_list,
            refresh_workers=args.refresh_workers,
            refresh_retries=args.refresh_retries,
            refresh_retry_backoff_seconds=args.refresh_retry_backoff_seconds,
            refresh_startup_jitter_seconds=args.refresh_startup_jitter_seconds,
        )

    if args.enrich or args.refresh_enrichment:
        sync_local_csv_sources()
        enrich_raw_sources(force_refresh=args.refresh_enrichment)

    rebuild_generated_data()
    print("Generated list data, manifests, and search index.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
