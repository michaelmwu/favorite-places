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
import sqlite3
import time
import unicodedata
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Any
from urllib.parse import parse_qsl, quote_plus, unquote, urlencode, urlsplit, urlunsplit
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pycountry
from pydantic import TypeAdapter
from PIL import Image, ImageOps, UnidentifiedImageError, features

ROOT = Path(__file__).resolve().parent.parent
SITE_DIR_ENV = "FAVORITE_PLACES_SITE_DIR"


def resolve_site_dir() -> Path:
    configured_site_dir = os.environ.get(SITE_DIR_ENV)
    if configured_site_dir:
        path = Path(configured_site_dir).expanduser()
        if not path.is_absolute():
            path = ROOT / path
        return path.resolve()

    default_site_dir = ROOT / "site"
    if default_site_dir.exists():
        return default_site_dir

    return ROOT / "site.example"


SITE_DIR = resolve_site_dir()
CONFIG_PATH = SITE_DIR / "list_sources.json"
RAW_DIR = SITE_DIR / "data" / "raw"
PLACES_CACHE_DIR = SITE_DIR / "data" / "cache" / "google-places"
PLACES_SQLITE_PATH = SITE_DIR / "data" / "cache" / "places.sqlite"
GENERATED_DIR = ROOT / "src" / "data" / "generated"
GENERATED_LISTS_DIR = GENERATED_DIR / "lists"
PUBLIC_DATA_DIR = ROOT / "public" / "data"
PLACE_PHOTOS_DIR = SITE_DIR / "public" / "place-photos"
LIST_OVERRIDES_DIR = SITE_DIR / "overrides" / "lists"
PLACE_OVERRIDES_DIR = SITE_DIR / "overrides" / "places"
SCRAPER_STATE_DIR = ROOT / ".context" / "gmaps-scraper"
AUTO_REFRESH_WORKER_CAP = 4
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
BEST_HIT_MAX_COUNT = 6
BEST_HIT_MIN_RATING = 4.5
BEST_HIT_MAX_RATING = 4.8
BEST_HIT_RATING_PERCENTILE = 0.8
BEST_HIT_MIN_CANDIDATE_COUNT = 3
BEST_HIT_REVIEW_THRESHOLD_MULTIPLIER = 0.35
BEST_HIT_MIN_REVIEW_THRESHOLD = 10
BEST_HIT_MAX_REVIEW_THRESHOLD = 2_000
BEST_HIT_RELAXED_REVIEW_THRESHOLD_MULTIPLIER = 0.65
PLACES_TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
PHOTO_DOWNLOAD_TIMEOUT_SECONDS = 20
PHOTO_CARD_WIDTH = 800
PHOTO_CARD_HEIGHT = 600
PHOTO_CARD_QUALITY = 78
# Bump this for any SQLite schema or row-derivation semantic change that must force a rebuild.
PLACES_SQLITE_SIGNATURE_VERSION = 3
PLACES_SQLITE_BUILD_METADATA_KEY = "build_signature"
STABLE_GENERATED_AT_FALLBACK = "1970-01-01T00:00:00+00:00"
SQLITE_UNREADABLE_ERROR_MARKERS = (
    "database disk image is malformed",
    "file is not a database",
    "malformed",
    "not a database",
)
PLACES_SQLITE_SCHEMA_SQL = """
CREATE TABLE build_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE guides (
    slug TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    source_url TEXT,
    list_id TEXT,
    country_name TEXT NOT NULL,
    country_code TEXT,
    city_name TEXT NOT NULL,
    list_tags_json TEXT NOT NULL,
    featured_place_ids_json TEXT NOT NULL,
    best_hit_place_ids_json TEXT NOT NULL,
    best_hit_min_rating REAL,
    best_hit_min_reviews INTEGER,
    top_categories_json TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    place_count INTEGER NOT NULL,
    center_lat REAL,
    center_lng REAL
);

CREATE TABLE canonical_places (
    place_id TEXT PRIMARY KEY,
    first_seen_guide_slug TEXT NOT NULL,
    raw_name TEXT,
    raw_address TEXT,
    raw_lat REAL,
    raw_lng REAL,
    raw_maps_url TEXT,
    raw_cid TEXT,
    raw_google_id TEXT,
    raw_maps_place_token TEXT,
    normalized_name TEXT NOT NULL,
    normalized_address TEXT,
    normalized_lat REAL,
    normalized_lng REAL,
    normalized_maps_url TEXT,
    normalized_cid TEXT,
    normalized_google_id TEXT,
    normalized_google_place_id TEXT,
    normalized_google_place_resource_name TEXT,
    normalized_rating REAL,
    normalized_user_rating_count INTEGER,
    normalized_primary_category TEXT,
    normalized_primary_category_localized TEXT,
    normalized_tags_json TEXT NOT NULL,
    normalized_vibe_tags_json TEXT NOT NULL,
    normalized_neighborhood TEXT,
    normalized_note TEXT,
    normalized_why_recommended TEXT,
    normalized_main_photo_path TEXT,
    normalized_top_pick INTEGER NOT NULL,
    normalized_hidden INTEGER NOT NULL,
    normalized_manual_rank INTEGER NOT NULL,
    normalized_status TEXT NOT NULL,
    cache_fetched_at TEXT,
    cache_last_verified_at TEXT,
    cache_refresh_after TEXT,
    cache_source TEXT,
    cache_query TEXT,
    cache_input_signature TEXT,
    cache_matched INTEGER,
    cache_score INTEGER,
    cache_error TEXT,
    cache_error_body TEXT,
    enrichment_google_place_id TEXT,
    enrichment_google_place_resource_name TEXT,
    enrichment_display_name TEXT,
    enrichment_formatted_address TEXT,
    enrichment_google_maps_uri TEXT,
    enrichment_rating REAL,
    enrichment_user_rating_count INTEGER,
    enrichment_primary_type TEXT,
    enrichment_primary_type_display_name TEXT,
    enrichment_primary_type_display_name_localized TEXT,
    enrichment_types_json TEXT NOT NULL,
    enrichment_business_status TEXT,
    enrichment_website TEXT,
    enrichment_phone TEXT,
    enrichment_plus_code TEXT,
    enrichment_description TEXT,
    enrichment_main_photo_url TEXT,
    enrichment_photo_url TEXT,
    enrichment_limited_view INTEGER
);

CREATE TABLE guide_enrichment_cache (
    guide_slug TEXT NOT NULL,
    place_id TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    last_verified_at TEXT,
    refresh_after TEXT,
    source TEXT,
    query TEXT NOT NULL,
    input_signature TEXT,
    matched INTEGER,
    score INTEGER,
    error TEXT,
    error_body TEXT,
    cache_json TEXT NOT NULL,
    PRIMARY KEY (guide_slug, place_id)
);

CREATE TABLE guide_places (
    guide_slug TEXT NOT NULL,
    sort_order INTEGER NOT NULL,
    place_id TEXT NOT NULL,
    is_featured INTEGER NOT NULL,
    is_best_hit INTEGER NOT NULL,
    place_name TEXT NOT NULL,
    neighborhood TEXT,
    primary_category TEXT,
    primary_category_localized TEXT,
    rating REAL,
    user_rating_count INTEGER,
    status TEXT NOT NULL,
    top_pick INTEGER NOT NULL,
    hidden INTEGER NOT NULL,
    manual_rank INTEGER NOT NULL,
    note TEXT,
    why_recommended TEXT,
    main_photo_path TEXT,
    maps_url TEXT NOT NULL,
    PRIMARY KEY (guide_slug, sort_order),
    FOREIGN KEY (guide_slug) REFERENCES guides(slug) ON DELETE CASCADE,
    FOREIGN KEY (place_id) REFERENCES canonical_places(place_id) ON DELETE CASCADE
);

CREATE TABLE place_photos (
    guide_slug TEXT NOT NULL,
    sort_order INTEGER NOT NULL,
    place_id TEXT NOT NULL,
    source_photo_url TEXT,
    local_path TEXT,
    fetch_status TEXT NOT NULL,
    last_fetched_at TEXT,
    content_sha256 TEXT,
    size_bytes INTEGER,
    PRIMARY KEY (guide_slug, sort_order),
    FOREIGN KEY (guide_slug, sort_order) REFERENCES guide_places(guide_slug, sort_order) ON DELETE CASCADE
);

CREATE INDEX idx_canonical_places_google_place_id
    ON canonical_places(enrichment_google_place_id);
CREATE INDEX idx_guide_enrichment_cache_place_id
    ON guide_enrichment_cache(place_id);
CREATE INDEX idx_guide_places_place_id
    ON guide_places(place_id);
CREATE INDEX idx_place_photos_local_path
    ON place_photos(local_path);
"""
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


@dataclass(frozen=True)
class PendingPhotoJob:
    guide_slug: str
    place_id: str
    place_name: str
    photo_url: str


@dataclass(frozen=True)
class PlacesSqliteRows:
    guide_rows: list[tuple[Any, ...]]
    canonical_place_rows: list[tuple[Any, ...]]
    guide_cache_rows: list[tuple[Any, ...]]
    guide_place_rows: list[tuple[Any, ...]]
    photo_rows: list[tuple[Any, ...]]


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
LOCATION_TAG_ALIASES: dict[str, tuple[str, ...]] = {
    "geneve": ("geneva",),
    "geneva": ("geneve",),
}
BROKEN_TAG_NORMALIZATION_MAP: dict[str, str] = {
    "gen-ve": "geneve",
}


def default_refresh_workers() -> int:
    cpu_count = os.cpu_count() or AUTO_REFRESH_WORKER_CAP
    return max(1, min(AUTO_REFRESH_WORKER_CAP, SCRAPER_SESSION_SLOT_COUNT, cpu_count))


DEFAULT_REFRESH_WORKERS = default_refresh_workers()


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


def terminate_executor(executor: Any) -> None:
    terminate_workers = getattr(executor, "terminate_workers", None)
    if callable(terminate_workers):
        terminate_workers()
        return

    kill_workers = getattr(executor, "kill_workers", None)
    if callable(kill_workers):
        kill_workers()
        return

    executor.shutdown(wait=False, cancel_futures=True)
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
GENERIC_ENRICHMENT_TYPE_TAGS = frozenset(
    {
        "establishment",
        "point-of-interest",
        "store",
        "premise",
        "subpremise",
        "street-address",
        "route",
        "political",
        "item",
        "local-guide",
    }
)
INVALID_ENRICHMENT_TYPE_TAG_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^\d+-reviews?$"),
)
INVALID_ENRICHMENT_PRIMARY_CATEGORY_DISPLAY_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^\d+\s+reviews?$", re.IGNORECASE),
    re.compile(r"^floor\s+\d+$", re.IGNORECASE),
    re.compile(r"^free cancellation\b", re.IGNORECASE),
)
PARENT_TYPE_TAG_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("restaurant", ("restaurant", "bistro", "diner", "grill", "eatery")),
    ("bar", ("bar", "pub", "brewery", "izakaya", "tavern", "speakeasy")),
    ("cafe", ("cafe", "coffee", "coffee-shop", "espresso-bar", "tea-house")),
    ("bakery", ("bakery", "patisserie", "pastry", "dessert", "donut", "ice-cream")),
    ("museum", ("museum", "art-gallery", "library", "archive", "planetarium")),
    ("attraction", ("tourist-attraction", "historical-landmark", "monument", "temple", "shrine", "church", "mosque", "synagogue")),
    ("park", ("park", "botanical-garden", "garden", "trailhead", "campground", "nature-preserve")),
    ("shopping", ("market", "boutique", "mall", "shopping-center")),
)
INFERRED_PARENT_TYPE_TAG_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "japanese-restaurant",
        (
            "japanese-restaurant",
            "sushi-restaurant",
            "ramen-restaurant",
            "udon-noodle-restaurant",
            "soba-noodle-restaurant",
            "izakaya-restaurant",
            "yakitori-restaurant",
            "yakiniku-restaurant",
            "kaiseki-restaurant",
            "tempura-restaurant",
            "teppanyaki-restaurant",
            "tonkatsu-restaurant",
            "okonomiyaki-restaurant",
            "japanese-curry-restaurant",
            "shabu-shabu-restaurant",
            "sukiyaki-restaurant",
            "ramen-restaurant",
            "kaiseki-restaurant",
            "curry-restaurant",
        ),
    ),
    (
        "western-restaurant",
        (
            "western-restaurant",
            "steak-house",
            "chophouse-restaurant",
            "continental-restaurant",
        ),
    ),
    (
        "chinese-restaurant",
        (
            "chinese-restaurant",
            "cantonese-restaurant",
            "sichuan-restaurant",
            "hot-pot-restaurant",
            "dumpling-restaurant",
            "chinese-noodle-restaurant",
            "dim-sum-restaurant",
            "hakka-restaurant",
        ),
    ),
    (
        "italian-restaurant",
        (
            "italian-restaurant",
            "pizza-restaurant",
            "pasta-shop",
            "trattoria",
            "osteria",
        ),
    ),
    ("mexican-restaurant", ("mexican-restaurant", "taco-restaurant", "taqueria")),
    ("korean-restaurant", ("korean-restaurant", "korean-barbecue-restaurant")),
)
DISPLAY_NAME_TYPE_TAG_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("cocktail-bar", ("cocktail bar", "雞尾酒酒吧", "鸡尾酒酒吧", "칵테일바", "カクテル バー")),
    ("beer-hall", ("beer hall", "啤酒館", "啤酒馆")),
    ("steak-house", ("steak house", "steakhouse", "ステーキハウス", "牛排館", "牛排馆", "스테이크하우스")),
    ("izakaya-restaurant", ("izakaya", "居酒屋")),
    ("ramen-restaurant", ("ramen", "ラーメン屋", "拉麵店")),
    ("chinese-noodle-restaurant", ("麵店", "冷麵店", "ร้านก๋วยเตี๋ยว")),
    ("sushi-restaurant", ("sushi", "壽司店", "寿司店", "스시집")),
    ("yakitori-restaurant", ("yakitori", "焼き鳥店", "串燒店", "串烧店")),
    ("soba-noodle-restaurant", ("soba", "蕎麦店", "荞麦面店")),
    ("udon-noodle-restaurant", ("udon", "うどん店", "乌冬面店", "烏龍麵店")),
    (
        "hot-pot-restaurant",
        ("hot pot", "火鍋餐廳", "火锅店", "鍋料理店", "涮涮鍋餐廳", "しゃぶしゃぶ店", "すき焼き/しゃぶしゃぶ店"),
    ),
    ("yakiniku-restaurant", ("yakiniku", "焼肉店", "燒肉餐廳", "烧肉店", "日式燒肉餐廳", "日式烧肉餐厅")),
    ("kaiseki-restaurant", ("kaiseki", "会席・懐石料理店")),
    ("teppanyaki-restaurant", ("teppanyaki", "鉄板焼き店")),
    ("curry-restaurant", ("curry restaurant", "カレー店")),
    ("barbecue-restaurant", ("barbecue restaurant", "燒烤", "숯불구이/바베큐전문점")),
    ("pizza-restaurant", ("pizza", "ピザ店")),
    ("hamburger-restaurant", ("hamburger", "burger", "ハンバーガー店", "漢堡店", "汉堡店")),
    ("dumpling-restaurant", ("dumpling", "餃子店", "饺子馆")),
    ("indian-restaurant", ("indian restaurant", "インド料理店")),
    ("american-restaurant", ("american restaurant", "アメリカ料理店")),
    ("italian-restaurant", ("italian restaurant", "イタリア料理店", "義大利餐廳", "意大利餐廳", "意大利餐厅")),
    ("french-restaurant", ("french restaurant", "フランス料理店", "法國餐廳", "法国餐厅")),
    ("vietnamese-restaurant", ("vietnamese restaurant", "ベトナム料理店")),
    ("thai-restaurant", ("thai restaurant", "タイ料理店", "ภัตตาคารอาหารไทย")),
    ("mediterranean-restaurant", ("mediterranean restaurant", "地中海料理店")),
    ("japanese-restaurant", ("japanese restaurant", "日本餐廳", "日本餐厅", "日本料理店", "和食店", "和食レストラン", "うなぎ料理店", "郷土料理店", "もんじゃ焼き屋", "定食屋")),
    ("taiwanese-restaurant", ("taiwanese restaurant", "台灣餐廳", "台湾餐厅", "台菜餐廳", "台菜餐厅", "台灣菜", "早餐店")),
    ("cantonese-restaurant", ("cantonese restaurant", "粵菜館", "粤菜馆")),
    ("sichuan-restaurant", ("sichuan restaurant", "四川酒家", "四川菜館", "四川菜馆")),
    ("chinese-restaurant", ("chinese restaurant", "中菜館", "中菜馆", "中餐館", "中餐馆", "中式麵食店", "中式面食店", "中華料理店", "酒樓", "浙菜/浙江菜館", "中式包點店")),
    ("seafood-restaurant", ("seafood restaurant", "海鮮餐廳", "海鲜餐厅", "シーフード・海鮮料理店", "海鲜馆", "ภัตตาคารอาหารทะเล")),
    ("fine-dining-restaurant", ("fine dining restaurant", "ภัตตาคารอาหารแบบหรูหรา")),
    ("mexican-restaurant", ("mexican restaurant", "墨西哥餐廳", "墨西哥餐厅", "メキシコ料理店")),
    ("vegetarian-restaurant", ("vegetarian restaurant", "素食餐廳", "素食餐厅")),
    ("asian-restaurant", ("asian restaurant", "亞洲菜餐廳", "亚洲菜餐厅")),
    ("hakka-restaurant", ("hakka restaurant", "客家菜館", "客家菜馆")),
    ("museum", ("museum", "博物館", "博物馆", "박물관")),
    ("art-gallery", ("art gallery", "art museum", "美術館", "美术馆", "畫廊", "画廊", "미술관", "アート ギャラリー")),
    ("aquarium", ("aquarium", "水族館")),
    ("cafe", ("coffee shop", "cafe", "咖啡店", "咖啡館", "咖啡馆", "カフェ・喫茶", "カフェ", "喫茶", "카페", "커피숍/커피 전문점")),
    ("tea-house", ("tea house", "茶藝館", "傳統茶館", "전통 찻집")),
    ("tea-store", ("tea store", "茶葉店")),
    ("bakery", ("bakery", "ベーカリー", "餅店", "糖果糕餅店", "和菓子屋", "甜品店")),
    ("ice-cream-shop", ("ice cream", "アイスクリーム店", "雪糕店")),
    ("bubble-tea-store", ("bubble tea", "珍珠奶茶店")),
    ("wine-bar", ("wine bar", "ワインバー", "紅酒吧", "红酒吧", "와인 바")),
    ("pub", ("pub", "パブ")),
    ("bar", ("bar", "酒吧", "バー", "술집", "บาร์")),
    ("restaurant", ("restaurant", "餐廳", "餐厅", "餐館", "餐馆", "レストラン", "음식점", "식당", "小餐館", "小餐馆", "ร้านอาหาร")),
    ("shopping-center", ("shopping center", "百貨公司")),
    ("market", ("market", "マーケット", "夜市")),
    ("park", ("park", "公園", "城市公園", "庭園")),
    ("garden", ("garden", "庭園")),
    ("trailhead", ("trail", "行山徑")),
    ("historical-landmark", ("historical landmark", "歷史建築", "史跡")),
    ("cathedral", ("cathedral", "カトリック大聖堂")),
    ("shrine", ("shrine", "神社")),
    ("tourist-attraction", ("tourist attraction", "旅遊景點", "旅游胜地", "観光名所", "관광 명소", "景勝地")),
    ("cultural-center", ("cultural center", "藝術中心", "艺术中心", "文化中心")),
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
        MarkerIcon,
        NormalizedPlace,
        PlacesSettings,
        PlaceField,
        PlaceProvenance,
        RawPlace,
        RawSavedList,
        SourceConfig,
    )
except ModuleNotFoundError:
    from scripts.pipeline_models import (
        EnrichmentCacheEntry,
        EnrichmentPlace,
        Guide,
        GuideManifest,
        MarkerIcon,
        NormalizedPlace,
        PlacesSettings,
        PlaceField,
        PlaceProvenance,
        RawPlace,
        RawSavedList,
        SourceConfig,
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
        help=(
            "Maximum parallel workers for headless refreshes, enrichment jobs, and optional photo refresh jobs. "
            "Defaults to an auto-derived value capped at 4; headed raw list refreshes stay single-worker."
        ),
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
        help="Maximum randomized delay before each Google list scrape, enrichment job, or optional photo refresh job starts.",
    )
    parser.add_argument(
        "--enrich",
        action="store_true",
        help="Fill missing or stale place enrichment cache entries from Google Maps place pages, prioritizing totally missing places first.",
    )
    parser.add_argument(
        "--enrich-missing",
        action="store_true",
        help="Fill only missing place enrichment cache entries, skipping expiry-based refreshes.",
    )
    parser.add_argument(
        "--refresh-enrichment",
        action="store_true",
        help="Force-refresh place enrichment cache entries for every place.",
    )
    parser.add_argument(
        "--refresh-photos",
        action="store_true",
        help="Download missing local optimized place photos from cached enrichment photo URLs.",
    )
    parser.add_argument(
        "--export-cache-json",
        action="store_true",
        help="Export the current SQLite-backed enrichment cache into per-guide JSON files for debugging.",
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
    refresh_jobs: list[tuple[SourceConfig, Path, bool, RawSavedList | None]] = []

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
        refresh_jobs.append((source, raw_path, backup_available, existing_payload))

    if not refresh_jobs:
        return

    effective_startup_jitter_seconds = (
        refresh_startup_jitter_seconds if not headed and len(refresh_jobs) > 1 else 0
    )
    max_workers = max(1, refresh_workers)
    if headed or len(refresh_jobs) == 1 or max_workers == 1:
        failures: list[str] = []
        for source, raw_path, backup_available, existing_payload in refresh_jobs:
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
            payload = preserve_existing_raw_saved_list(
                source=source,
                slug=source.slug,
                existing_payload=existing_payload,
                refreshed_payload=payload,
            )
            write_json(raw_path, payload)
        if failures:
            failure_text = "\n".join(failures)
            raise RuntimeError(f"Raw refresh failed for {len(failures)} source(s):\n{failure_text}")
        return

    max_workers = min(max_workers, len(refresh_jobs))
    print(f"Running {len(refresh_jobs)} headless refresh jobs with {max_workers} workers")
    failures: list[str] = []

    executor = ThreadPoolExecutor(max_workers=max_workers)
    try:
        future_map = {
            executor.submit(
                scrape_google_list_url_with_retries,
                source,
                headed=False,
                refresh_retries=refresh_retries,
                refresh_retry_backoff_seconds=refresh_retry_backoff_seconds,
                refresh_startup_jitter_seconds=effective_startup_jitter_seconds,
            ): (source, raw_path, backup_available, existing_payload)
            for source, raw_path, backup_available, existing_payload in refresh_jobs
        }
        for future in as_completed(future_map):
            source, raw_path, backup_available, existing_payload = future_map[future]
            try:
                payload = future.result()
            except RECOVERABLE_REFRESH_ERRORS as exc:
                if backup_available:
                    print(f"Keeping existing raw snapshot for {source.slug} after refresh failure: {exc}")
                else:
                    failures.append(f"{source.slug}: {exc}")
                continue
            payload = preserve_existing_raw_saved_list(
                source=source,
                slug=source.slug,
                existing_payload=existing_payload,
                refreshed_payload=payload,
            )
            write_json(raw_path, payload)
    except KeyboardInterrupt:
        print("Interrupt received; terminating refresh workers.", flush=True)
        terminate_executor(executor)
        raise
    finally:
        executor.shutdown(wait=True, cancel_futures=True)

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


def metadata_datetime_or_none(value: str | None) -> datetime | None:
    normalized = as_string(value)
    if normalized is None:
        return None
    try:
        return parse_metadata_datetime(normalized)
    except ValueError:
        return None


def stable_generated_at(
    raw: RawSavedList,
    enrichment_cache: dict[str, EnrichmentCacheEntry],
    *,
    photo_paths: list[str | None] | None = None,
) -> str:
    candidates: list[datetime] = []

    raw_fetched_at = metadata_datetime_or_none(raw.fetched_at)
    if raw_fetched_at is not None:
        candidates.append(raw_fetched_at)

    for entry in enrichment_cache.values():
        verified_at = metadata_datetime_or_none(entry.last_verified_at)
        if verified_at is not None:
            candidates.append(verified_at)
            continue

        fetched_at = metadata_datetime_or_none(entry.fetched_at)
        if fetched_at is not None:
            candidates.append(fetched_at)

    for public_path in photo_paths or []:
        absolute_path = public_asset_absolute_path(public_path)
        if absolute_path is None or not absolute_path.exists():
            continue
        candidates.append(datetime.fromtimestamp(absolute_path.stat().st_mtime, tz=UTC))

    if not candidates:
        return STABLE_GENERATED_AT_FALLBACK
    return max(candidates).isoformat()


def rebuild_generated_data(
    *,
    refresh_photos: bool = False,
    photo_workers: int = DEFAULT_REFRESH_WORKERS,
    startup_jitter_seconds: float = DEFAULT_REFRESH_STARTUP_JITTER_SECONDS,
) -> None:
    sync_local_csv_sources()
    if GENERATED_LISTS_DIR.exists():
        shutil.rmtree(GENERATED_LISTS_DIR)
    GENERATED_LISTS_DIR.mkdir(parents=True, exist_ok=True)
    PUBLIC_DATA_DIR.mkdir(parents=True, exist_ok=True)
    PLACE_PHOTOS_DIR.mkdir(parents=True, exist_ok=True)
    guides: list[Guide] = []
    raw_lists: dict[str, RawSavedList] = {}
    enrichment_caches: dict[str, dict[str, EnrichmentCacheEntry]] = {}

    for raw_path in sorted(RAW_DIR.glob("*.json")):
        raw = RawSavedList.model_validate_json(raw_path.read_text(encoding="utf-8"))
        raw_lists[raw_path.stem] = raw
        enrichment_cache = load_places_cache(raw_path.stem)
        enrichment_caches[raw_path.stem] = enrichment_cache
        guide = normalize_guide(raw_path.stem, raw, enrichment_cache=enrichment_cache)
        guides.append(guide)

    populate_place_photos_for_guides(
        guides,
        enrichment_caches=enrichment_caches,
        refresh_photos=refresh_photos,
        photo_workers=photo_workers,
        startup_jitter_seconds=startup_jitter_seconds,
    )
    for guide in guides:
        guide.generated_at = stable_generated_at(
            raw_lists[guide.slug],
            enrichment_caches.get(guide.slug, {}),
            photo_paths=[place.main_photo_path for place in guide.places],
        )
    rebuild_places_sqlite(
        raw_lists=raw_lists,
        guides=guides,
        enrichment_caches=enrichment_caches,
    )

    for guide in guides:
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


def expand_location_tag_aliases(tags: set[str]) -> set[str]:
    expanded = set(tags)
    for tag in list(tags):
        expanded.update(LOCATION_TAG_ALIASES.get(tag, ()))
    return expanded


def normalize_guide(slug: str, raw: RawSavedList, *, enrichment_cache: dict[str, EnrichmentCacheEntry]) -> Guide:
    list_override = read_json(LIST_OVERRIDES_DIR / f"{slug}.json")
    place_override_map = read_json(PLACE_OVERRIDES_DIR / f"{slug}.json")

    title = as_string(list_override.get("title")) or raw.title or slug.replace("-", " ").title()
    description = as_string(list_override.get("description")) or raw.description

    city_name = as_string(list_override.get("city_name")) or infer_city_name(title)
    country_name = as_string(list_override.get("country_name")) or infer_country_name(title, raw)
    country_code = as_string(list_override.get("country_code")) or infer_country_code(country_name)

    description_tags = extract_hashtags(description)
    override_tags = [
        normalized
        for tag in coerce_string_list(list_override.get("list_tags"))
        if (normalized := normalize_list_tag(tag)) != "item"
    ]
    list_tags = sorted(expand_location_tag_aliases({*description_tags, *override_tags}))

    normalized_places: list[NormalizedPlace] = []
    category_counter: Counter[str] = Counter()
    prefer_enrichment_names = raw.configured_source_type == "google_export_csv"

    for place in raw.places:
        place_id = stable_place_id(place, source_type=raw.configured_source_type)
        override = place_override_map.get(place_id, {})
        enrichment_cache_entry = enrichment_cache.get(place_id)
        enrichment = coerce_enrichment_place(enrichment_cache_entry)
        manual_primary_category = as_string(override.get("primary_category"))
        primary_category = (
            manual_primary_category
            or enrichment.primary_type_display_name
            or humanize_type_id(enrichment.primary_type)
        )
        primary_category_localized = None if manual_primary_category else enrichment.primary_type_display_name_localized
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
        normalized_name = as_string(override.get("name")) or preferred_name
        normalized_address = place.address or enrichment.formatted_address
        maps_url = build_public_google_maps_url(
            name=normalized_name,
            address=normalized_address,
            lat=place.lat,
            lng=place.lng,
            raw_maps_url=place.maps_url,
            google_maps_uri=enrichment.google_maps_uri,
            google_place_id=enrichment.google_place_id,
        )

        normalized = NormalizedPlace(
            id=place_id,
            name=normalized_name,
            address=normalized_address,
            lat=place.lat,
            lng=place.lng,
            maps_url=maps_url,
            cid=place.cid,
            google_id=place.google_id,
            google_place_id=enrichment.google_place_id,
            google_place_resource_name=enrichment.google_place_resource_name,
            rating=enrichment.rating,
            user_rating_count=enrichment.user_rating_count,
            primary_category=primary_category,
            primary_category_localized=primary_category_localized,
            marker_icon=marker_icon,
            tags=tags,
            vibe_tags=vibe_tags,
            neighborhood=neighborhood,
            note=note,
            why_recommended=why_recommended,
            main_photo_path=None,
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
            primary_category_localized=primary_category_localized,
            tags=tags,
            city_name=city_name,
            top_pick_override=top_pick_override,
            status=status,
            prefer_enrichment_names=prefer_enrichment_names,
        )
        normalized_places.append(normalized)
        if primary_category and not place_is_permanently_closed(normalized):
            category_counter[primary_category] += 1

    normalized_places.sort(
        key=lambda place: (
            place.hidden,
            not place.top_pick,
            -place.manual_rank,
            place.name.lower(),
        )
    )

    display_places = [place for place in normalized_places if place_is_visible_in_ui(place)]

    featured_place_ids = [
        place_id
        for place_id in coerce_string_list(list_override.get("featured_place_ids"))
        if any(place.id == place_id for place in display_places)
    ]
    auto_featured_ids = [place.id for place in display_places if place.top_pick]
    if featured_place_ids:
        featured_place_ids = list(dict.fromkeys([*featured_place_ids, *auto_featured_ids]))[:3]
    else:
        featured_place_ids = auto_featured_ids[:3]
    best_hit_place_ids, best_hit_min_rating, best_hit_min_reviews = select_best_hit_place_ids(
        display_places,
        featured_place_ids=featured_place_ids,
    )

    top_categories = [name for name, _count in category_counter.most_common(4)]
    generated_at = stable_generated_at(raw, enrichment_cache)
    guide_center = guide_location_center(display_places)
    warn_far_map_pins(slug, display_places, guide_center)

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
        best_hit_place_ids=best_hit_place_ids,
        best_hit_min_rating=best_hit_min_rating,
        best_hit_min_reviews=best_hit_min_reviews,
        top_categories=top_categories,
        generated_at=generated_at,
        place_count=len(display_places),
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
    primary_category_localized: str | None,
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
    if normalized.rating is not None:
        provenance.rating = google_places_field(normalized.rating, enrichment_cache_entry)
    if normalized.user_rating_count is not None:
        provenance.user_rating_count = google_places_field(normalized.user_rating_count, enrichment_cache_entry)
    if primary_category:
        provenance.primary_category = (
            manual_place_field(primary_category)
            if manual_category
            else google_places_field(primary_category, enrichment_cache_entry)
        )
    if primary_category_localized:
        provenance.primary_category_localized = google_places_field(primary_category_localized, enrichment_cache_entry)
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
        tag = normalize_tag_slug(city_name)
        put_tag(tag, google_list_field(tag, raw), priority=10)
    for tag in derive_locality_tags(
        raw_place.address,
        city_name=city_name,
        enrichment=enrichment,
        category=primary_category,
    ):
        put_tag(tag, google_list_field(tag, raw), priority=10)
    if primary_category:
        tag = normalize_tag_slug(primary_category)
        put_tag(
            tag,
            source_place_field(tag, primary_category_field),
            priority=30 if primary_category_field and primary_category_field.source == "manual" else 20,
        )
    for tag in derive_enrichment_type_tags(enrichment):
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


def place_is_permanently_closed(place: NormalizedPlace) -> bool:
    return place.status == "closed-permanently"


def place_is_visible_in_ui(place: NormalizedPlace) -> bool:
    return not place.hidden and not place_is_permanently_closed(place)


def percentile(values: list[int], rank: float) -> float | None:
    if not values:
        return None

    sorted_values = sorted(values)
    if len(sorted_values) == 1:
        return float(sorted_values[0])

    clamped_rank = min(max(rank, 0.0), 1.0)
    index = (len(sorted_values) - 1) * clamped_rank
    lower_index = math.floor(index)
    upper_index = math.ceil(index)
    lower_value = sorted_values[lower_index]
    upper_value = sorted_values[upper_index]
    if lower_index == upper_index:
        return float(lower_value)

    fraction = index - lower_index
    return lower_value + (upper_value - lower_value) * fraction


def round_review_threshold(value: float) -> int:
    if value <= 25:
        step = 5
    elif value <= 100:
        step = 10
    elif value <= 500:
        step = 25
    elif value <= 2_000:
        step = 50
    else:
        step = 100
    return max(step, int(math.ceil(value / step) * step))


def round_rating_floor(value: float) -> float:
    clamped_value = min(BEST_HIT_MAX_RATING, max(BEST_HIT_MIN_RATING, value))
    return round(math.floor((clamped_value + 1e-9) * 10) / 10, 1)


def guide_best_hit_review_threshold(places: list[NormalizedPlace]) -> int | None:
    review_counts = sorted(
        place.user_rating_count
        for place in places
        if place.rating is not None and place.user_rating_count is not None and place.user_rating_count > 0
    )
    if not review_counts:
        return None

    review_count_percentile = percentile(review_counts, 0.75)
    if review_count_percentile is None:
        return None

    adaptive_threshold = review_count_percentile * BEST_HIT_REVIEW_THRESHOLD_MULTIPLIER
    clamped_threshold = min(
        BEST_HIT_MAX_REVIEW_THRESHOLD,
        max(BEST_HIT_MIN_REVIEW_THRESHOLD, adaptive_threshold),
    )
    return round_review_threshold(clamped_threshold)


def guide_best_hit_score(
    place: NormalizedPlace,
    *,
    baseline_rating: float,
    prior_weight: int,
) -> float:
    rating = place.rating
    review_count = place.user_rating_count
    if rating is None or review_count is None or review_count <= 0:
        return 0.0

    return ((review_count * rating) + (prior_weight * baseline_rating)) / (review_count + prior_weight)


def guide_best_hit_rating_floor(places: list[NormalizedPlace]) -> float:
    ratings = sorted(
        place.rating
        for place in places
        if place.rating is not None and place.user_rating_count is not None and place.user_rating_count > 0
    )
    if not ratings:
        return BEST_HIT_MIN_RATING

    percentile_rating = percentile(ratings, BEST_HIT_RATING_PERCENTILE)
    if percentile_rating is None:
        return BEST_HIT_MIN_RATING

    return round_rating_floor(percentile_rating)


def select_best_hit_place_ids(
    places: list[NormalizedPlace],
    *,
    featured_place_ids: list[str],
) -> tuple[list[str], float | None, int | None]:
    featured_ids = set(featured_place_ids)
    rated_places = [
        place
        for place in places
        if place.rating is not None and place.user_rating_count is not None and place.user_rating_count > 0
    ]
    if not rated_places:
        return ([], None, None)

    review_threshold = guide_best_hit_review_threshold(rated_places)
    baseline_rating = sum(place.rating for place in rated_places if place.rating is not None) / len(rated_places)

    def matching_places(min_reviews: int, rating_floor: float) -> list[NormalizedPlace]:
        return [
            place
            for place in rated_places
            if place.id not in featured_ids
            and place.rating is not None
            and place.rating >= rating_floor
            and (place.user_rating_count or 0) >= min_reviews
        ]

    applied_review_threshold = review_threshold or BEST_HIT_MIN_REVIEW_THRESHOLD
    relaxed_review_threshold = applied_review_threshold
    if review_threshold is not None:
        relaxed_review_threshold = max(
            BEST_HIT_MIN_REVIEW_THRESHOLD,
            round_review_threshold(review_threshold * BEST_HIT_RELAXED_REVIEW_THRESHOLD_MULTIPLIER),
        )

    review_qualified_places = matching_places(applied_review_threshold, BEST_HIT_MIN_RATING)
    if len(review_qualified_places) < BEST_HIT_MIN_CANDIDATE_COUNT and relaxed_review_threshold < applied_review_threshold:
        applied_review_threshold = relaxed_review_threshold
        review_qualified_places = matching_places(applied_review_threshold, BEST_HIT_MIN_RATING)

    if not review_qualified_places:
        return ([], None, None)

    rating_floor = guide_best_hit_rating_floor(review_qualified_places)
    candidates = matching_places(applied_review_threshold, rating_floor)
    while (
        rating_floor > BEST_HIT_MIN_RATING
        and len(candidates) < BEST_HIT_MIN_CANDIDATE_COUNT
    ):
        rating_floor = round_rating_floor(rating_floor - 0.1)
        candidates = matching_places(applied_review_threshold, rating_floor)

    if len(candidates) < BEST_HIT_MAX_COUNT and review_threshold is not None:
        if relaxed_review_threshold < applied_review_threshold:
            relaxed_candidates = matching_places(relaxed_review_threshold, rating_floor)
            if len(relaxed_candidates) > len(candidates):
                candidates = relaxed_candidates
                applied_review_threshold = relaxed_review_threshold

    if not candidates:
        return ([], None, None)

    scored_candidates = sorted(
        candidates,
        key=lambda place: (
            -(place.rating or 0.0),
            -guide_best_hit_score(place, baseline_rating=baseline_rating, prior_weight=applied_review_threshold),
            -(place.user_rating_count or 0),
            -place.manual_rank,
            place.name.lower(),
        ),
    )
    return (
        [place.id for place in scored_candidates[:BEST_HIT_MAX_COUNT]],
        rating_floor,
        applied_review_threshold,
    )


def summarize_guide(guide: Guide) -> GuideManifest:
    featured_names = [
        place.name
        for place in guide.places
        if place.id in set(guide.featured_place_ids) and place_is_visible_in_ui(place)
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


def enrich_raw_sources(
    *,
    force_refresh: bool,
    missing_only: bool = False,
    refresh_workers: int = DEFAULT_REFRESH_WORKERS,
    refresh_startup_jitter_seconds: float = DEFAULT_REFRESH_STARTUP_JITTER_SECONDS,
) -> None:
    api_key = google_places_api_key()
    cache_payloads: dict[str, dict[str, EnrichmentCacheEntry]] = {}
    enrich_jobs: list[tuple[str, str, str, str, dict[str, Any]]] = []

    for raw_path in sorted(RAW_DIR.glob("*.json")):
        raw = RawSavedList.model_validate_json(raw_path.read_text(encoding="utf-8"))
        cache_payload = load_places_cache(raw_path.stem)
        cache_payloads[raw_path.stem] = cache_payload
        for place in raw.places:
            place_id = stable_place_id(place, source_type=raw.configured_source_type)
            refresh_reason = enrichment_refresh_reason(
                place,
                cache_payload.get(place_id),
                force_refresh=force_refresh,
                missing_only=missing_only,
            )
            if refresh_reason is None:
                continue

            enrich_jobs.append(
                (
                    raw_path.stem,
                    place_id,
                    place.name,
                    refresh_reason,
                    place.model_dump(mode="json"),
                )
            )

    if not enrich_jobs:
        return

    enrich_jobs.sort(key=enrichment_job_priority)

    effective_startup_jitter_seconds = (
        refresh_startup_jitter_seconds if len(enrich_jobs) > 1 else 0
    )
    max_workers = max(1, min(refresh_workers, len(enrich_jobs)))
    if max_workers == 1 or len(enrich_jobs) == 1:
        for slug, place_id, place_name, refresh_reason, place_payload in enrich_jobs:
            entry = enrich_place_job(
                slug,
                place_id,
                place_name,
                refresh_reason,
                place_payload,
                api_key=api_key,
                refresh_startup_jitter_seconds=effective_startup_jitter_seconds,
                existing_entry=cache_payloads[slug].get(place_id),
            )
            cache_payloads[slug][place_id] = entry
            save_places_cache(slug, cache_payloads[slug])
        return

    print(f"Running {len(enrich_jobs)} enrichment jobs with {max_workers} workers")
    executor = ThreadPoolExecutor(max_workers=max_workers)
    try:
        future_map = {
            executor.submit(
                enrich_place_job,
                slug,
                place_id,
                place_name,
                refresh_reason,
                place_payload,
                api_key=api_key,
                refresh_startup_jitter_seconds=effective_startup_jitter_seconds,
                existing_entry=cache_payloads[slug].get(place_id),
            ): (slug, place_id)
            for slug, place_id, place_name, refresh_reason, place_payload in enrich_jobs
        }
        for future in as_completed(future_map):
            slug, place_id = future_map[future]
            cache_payloads[slug][place_id] = future.result()
            save_places_cache(slug, cache_payloads[slug])
    except KeyboardInterrupt:
        print("Interrupt received; terminating enrichment workers.", flush=True)
        terminate_executor(executor)
        raise
    finally:
        executor.shutdown(wait=True, cancel_futures=True)


def enrich_place_job(
    slug: str,
    place_id: str,
    place_name: str,
    refresh_reason: str,
    place_payload: dict[str, Any],
    *,
    api_key: str | None,
    refresh_startup_jitter_seconds: float,
    existing_entry: EnrichmentCacheEntry | None = None,
) -> EnrichmentCacheEntry:
    print(
        f"Enriching {slug}:{place_id} [{place_name}] ({refresh_reason})",
        flush=True,
    )
    sleep_for_refresh_startup_jitter(refresh_startup_jitter_seconds)
    place = RawPlace.model_validate(place_payload)
    refreshed_entry = fetch_places_enrichment(place, api_key=api_key)
    merged_entry, warning = preserve_existing_enrichment(
        slug=slug,
        place_id=place_id,
        place_name=place_name,
        existing_entry=existing_entry,
        refreshed_entry=refreshed_entry,
    )
    if warning is not None:
        print(warning, flush=True)
    return merged_entry


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
        tags.add(normalize_tag_slug(city_name))
    tags.update(
        derive_locality_tags(
            place.address,
            city_name=city_name,
            enrichment=enrichment,
            category=category,
        )
    )
    if category:
        tags.add(normalize_tag_slug(category))
    tags.update(derive_enrichment_type_tags(enrichment))
    return sorted(tag for tag in tags if tag)


def derive_locality_tags(
    address: str | None,
    *,
    city_name: str | None,
    enrichment: EnrichmentPlace,
    category: str | None,
) -> list[str]:
    locality_tags: list[str] = []
    for locality in infer_address_localities(address, city_name=city_name):
        tag = normalize_tag_slug(locality)
        if tag:
            append_unique_tag(locality_tags, tag)

    if not locality_tags:
        return []

    has_specific_enrichment = bool(category or derive_enrichment_type_tags(enrichment))
    if has_specific_enrichment:
        return locality_tags[:1]
    return locality_tags[:2]


def derive_enrichment_type_tags(enrichment: EnrichmentPlace) -> list[str]:
    _primary_type, type_ids = normalized_enrichment_type_ids(
        enrichment.primary_type,
        enrichment.primary_type_display_name,
        enrichment.types,
    )
    return [type_id.replace("_", "-") for type_id in type_ids]


def normalize_enrichment_type_tag(value: str | None) -> str | None:
    if not value:
        return None
    tag = slugify(value.replace("_", "-"))
    if (
        not tag
        or tag in GENERIC_ENRICHMENT_TYPE_TAGS
        or any(pattern.match(tag) for pattern in INVALID_ENRICHMENT_TYPE_TAG_PATTERNS)
    ):
        return None
    return tag


def sanitize_enrichment_primary_category(value: str | None) -> str | None:
    normalized = as_string(value)
    if normalized is None:
        return None
    normalized = " ".join(normalized.split())
    tag = slugify(normalized)
    has_ascii_alnum = bool(re.search(r"[a-zA-Z0-9]", normalized))
    if (
        any(pattern.match(normalized) for pattern in INVALID_ENRICHMENT_PRIMARY_CATEGORY_DISPLAY_PATTERNS)
        or (has_ascii_alnum and tag in GENERIC_ENRICHMENT_TYPE_TAGS)
        or (has_ascii_alnum and any(pattern.match(tag) for pattern in INVALID_ENRICHMENT_TYPE_TAG_PATTERNS))
    ):
        return None
    return normalized


def looks_english_category_label(value: str | None) -> bool:
    return bool(value and value.isascii() and re.search(r"[A-Za-z]", value))


def humanize_type_id(value: str | None) -> str | None:
    normalized = as_string(value)
    if normalized is None:
        return None
    phrase = normalized.replace("_", " ").replace("-", " ").strip().lower()
    if not phrase:
        return None
    return phrase[:1].upper() + phrase[1:]


def canonical_primary_category_label(
    *,
    primary_type: str | None,
    display_name: str | None,
) -> str | None:
    sanitized_display_name = sanitize_enrichment_primary_category(display_name)
    if looks_english_category_label(sanitized_display_name):
        return sanitized_display_name
    return humanize_type_id(primary_type)


def localized_primary_category_label(
    *,
    raw_display_name: str | None,
    canonical_display_name: str | None,
) -> str | None:
    sanitized_display_name = sanitize_enrichment_primary_category(raw_display_name)
    if sanitized_display_name is None or looks_english_category_label(sanitized_display_name):
        return None
    if (
        canonical_display_name
        and normalize_type_lookup_text(sanitized_display_name) == normalize_type_lookup_text(canonical_display_name)
    ):
        return None
    return sanitized_display_name


def canonicalize_enrichment_place(place: EnrichmentPlace | None) -> EnrichmentPlace | None:
    if place is None:
        return None

    raw_display_name = place.primary_type_display_name_localized or place.primary_type_display_name
    canonical_display_name = canonical_primary_category_label(
        primary_type=place.primary_type,
        display_name=place.primary_type_display_name,
    )
    localized_display_name = localized_primary_category_label(
        raw_display_name=raw_display_name,
        canonical_display_name=canonical_display_name,
    )

    place.primary_type_display_name = canonical_display_name
    place.primary_type_display_name_localized = localized_display_name
    return place


def normalized_enrichment_type_ids(
    primary_type: str | None,
    primary_type_display_name: str | None,
    raw_types: list[str] | None,
) -> tuple[str | None, list[str]]:
    specific_tags: list[str] = []
    raw_type_ids = normalize_enrichment_type_ids_with_generic_fallback(raw_types or [])
    normalized_primary_type = normalize_enrichment_type_tag(primary_type)
    if normalized_primary_type:
        append_unique_tag(specific_tags, normalized_primary_type)
    for raw_value in raw_types or []:
        normalized_type_tag = normalize_enrichment_type_tag(raw_value)
        if normalized_type_tag:
            append_unique_tag(specific_tags, normalized_type_tag)
    for inferred_tag in infer_display_name_type_tags(primary_type_display_name):
        append_unique_tag(specific_tags, inferred_tag)

    if not specific_tags:
        fallback_primary = normalize_enrichment_type_id_with_generic_fallback(primary_type)
        fallback_types = raw_type_ids[:]
        if fallback_primary and fallback_primary not in fallback_types:
            fallback_types.insert(0, fallback_primary)
        return fallback_primary, fallback_types

    expanded_type_tags: list[str] = []
    for tag in specific_tags:
        for parent_tag in expanded_parent_type_tags(tag):
            append_unique_tag(expanded_type_tags, parent_tag)
        append_unique_tag(expanded_type_tags, tag)

    return (
        specific_tags[0].replace("-", "_"),
        [tag.replace("-", "_") for tag in expanded_type_tags],
    )


def normalize_enrichment_type_id_with_generic_fallback(value: str | None) -> str | None:
    if not value:
        return None
    normalized = slugify(value.replace("_", "-"))
    if not normalized or any(pattern.match(normalized) for pattern in INVALID_ENRICHMENT_TYPE_TAG_PATTERNS):
        return None
    return normalized.replace("-", "_")


def normalize_enrichment_type_ids_with_generic_fallback(values: list[str]) -> list[str]:
    normalized_ids: list[str] = []
    for value in values:
        normalized = normalize_enrichment_type_id_with_generic_fallback(value)
        if normalized and normalized not in normalized_ids:
            normalized_ids.append(normalized)
    return normalized_ids


def append_unique_tag(tags: list[str], candidate: str) -> None:
    if candidate and candidate not in tags:
        tags.append(candidate)


def expanded_parent_type_tags(tag: str) -> list[str]:
    parents: list[str] = []
    for parent_tag, phrases in PARENT_TYPE_TAG_RULES:
        if any(slug_phrase_matches(tag, phrase) for phrase in phrases):
            append_unique_tag(parents, parent_tag)
    if tag.endswith("-restaurant"):
        append_unique_tag(parents, "restaurant")
    for parent_tag, phrases in INFERRED_PARENT_TYPE_TAG_RULES:
        if any(slug_phrase_matches(tag, phrase) for phrase in phrases):
            if parent_tag.endswith("-restaurant"):
                append_unique_tag(parents, "restaurant")
            append_unique_tag(parents, parent_tag)
    return parents


def infer_display_name_type_tags(value: str | None) -> list[str]:
    if not value:
        return []
    tags: list[str] = []
    for tag, aliases in DISPLAY_NAME_TYPE_TAG_RULES:
        if any(type_alias_matches(value, alias) for alias in aliases):
            append_unique_tag(tags, tag)
    return tags


def type_alias_matches(label: str, alias: str) -> bool:
    if any(ord(char) > 127 for char in alias):
        return normalize_type_lookup_text(alias) in normalize_type_lookup_text(label)
    return slug_phrase_matches(slugify(label.replace("_", "-")), alias)


def normalize_type_lookup_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("_", " ").replace("-", " ").lower()).strip()


def derive_marker_icon(
    place: RawPlace,
    *,
    enrichment: EnrichmentPlace,
    category: str | None,
    note: str | None,
    why_recommended: str | None,
) -> MarkerIcon:
    candidate_slugs = [
        slugify(term.replace("_", "-"))
        for term in [
            category,
            enrichment.primary_type,
            enrichment.primary_type_display_name,
            *enrichment.types,
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
                    place.note,
                    note,
                    why_recommended,
                    category,
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
    generated_at = max(
        (guide.generated_at for guide in guides if as_string(guide.generated_at) is not None),
        default=STABLE_GENERATED_AT_FALLBACK,
    )
    return {
        "version": 1,
        "generated_at": generated_at,
        "guides": [search_index_guide_entry(guide) for guide in guides],
        "entries": [
            search_index_place_entry(guide, place)
            for guide in guides
            for place in guide.places
            if place_is_visible_in_ui(place)
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
            if place.id in featured_ids and place_is_visible_in_ui(place)
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
        "rating": place.rating,
        "user_rating_count": place.user_rating_count,
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
    normalized_tags = {
        normalize_list_tag(match.group(1))
        for match in re.finditer(r"#([^\s#]+)", text)
    }
    return sorted(tag for tag in normalized_tags if tag and tag != "item")


def normalize_list_tag(value: str) -> str:
    return normalize_tag_slug(value)


def normalize_tag_slug(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_value = "".join(char for char in normalized if not unicodedata.combining(char))
    slug = slugify(ascii_value)
    return BROKEN_TAG_NORMALIZATION_MAP.get(slug, slug)


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
    payload = normalize_json_payload(payload)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    if compact:
        text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    else:
        text = json.dumps(payload, indent=2, ensure_ascii=False)
    serialized = f"{text}\n"
    if path.exists() and path.read_text(encoding="utf-8") == serialized:
        return
    tmp_path.write_text(serialized, encoding="utf-8")
    tmp_path.replace(path)


def normalize_json_payload(payload: Any) -> Any:
    if hasattr(payload, "model_dump"):
        return normalize_json_payload(payload.model_dump(mode="json"))
    if isinstance(payload, dict):
        return {key: normalize_json_payload(value) for key, value in payload.items()}
    if isinstance(payload, list):
        return [normalize_json_payload(item) for item in payload]
    if isinstance(payload, tuple):
        return [normalize_json_payload(item) for item in payload]
    return payload


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
        site_relative_path = SITE_DIR / path
        root_relative_path = ROOT / path
        path = root_relative_path if root_relative_path.exists() and not site_relative_path.exists() else site_relative_path
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


def raw_place_match_keys(place: RawPlace, *, source_type: str | None = None) -> list[str]:
    keys: list[str] = []

    cid = as_string(place.cid) or extract_maps_cid(place.maps_url)
    if cid:
        keys.append(f"cid:{cid}")

    google_id = as_string(place.google_id)
    if google_id:
        keys.append(f"gid:{google_id.strip('/').replace('/', '-')}")

    maps_place_token = place.maps_place_token or extract_maps_place_token(place.maps_url)
    if maps_place_token:
        keys.append(f"gms:{maps_place_token}")

    name = as_string(place.name)
    lat = as_float(place.lat)
    lng = as_float(place.lng)
    if name and lat is not None and lng is not None:
        keys.append(f"name-ll:{slugify(name)}:{lat:.6f}:{lng:.6f}")

    address = as_string(place.address)
    if name and address:
        keys.append(f"name-address:{slugify(name)}:{slugify(address)}")

    if source_type == "google_export_csv":
        maps_url_id = short_maps_url_id(place.maps_url)
        if maps_url_id:
            keys.append(maps_url_id)

    keys.append(stable_place_id(place, source_type=source_type))
    return list(dict.fromkeys(keys))


def build_raw_place_preservation_index(
    raw: RawSavedList,
    *,
    source_type: str | None = None,
) -> dict[str, RawPlace]:
    index: dict[str, RawPlace] = {}
    for place in raw.places:
        for key in raw_place_match_keys(place, source_type=source_type):
            index.setdefault(key, place)
    return index


def preserve_existing_raw_place(
    *,
    existing_place: RawPlace,
    refreshed_place: RawPlace,
) -> tuple[RawPlace, list[str]]:
    preserved_fields: list[str] = []
    updates: dict[str, Any] = {}
    existing_name = as_string(existing_place.name)
    refreshed_name = as_string(refreshed_place.name)
    names_compatible = raw_place_names_are_compatible(existing_name, refreshed_name)

    if (
        names_compatible
        and not refreshed_place.address
        and raw_place_address_is_preservable(existing_place, refreshed_place)
    ):
        updates["address"] = existing_place.address
        preserved_fields.append("address")
    if names_compatible and not refreshed_place.google_id and existing_place.google_id:
        updates["google_id"] = existing_place.google_id
        preserved_fields.append("google_id")
    if names_compatible and not refreshed_place.cid and existing_place.cid:
        updates["cid"] = existing_place.cid
        preserved_fields.append("cid")
    if names_compatible and not refreshed_place.maps_place_token and existing_place.maps_place_token:
        updates["maps_place_token"] = existing_place.maps_place_token
        preserved_fields.append("maps_place_token")
    if names_compatible and not refreshed_place.is_favorite and existing_place.is_favorite:
        updates["is_favorite"] = True
        preserved_fields.append("is_favorite")

    if not updates:
        return refreshed_place, preserved_fields

    return refreshed_place.model_copy(update=updates), preserved_fields


def raw_place_names_are_compatible(existing_name: str | None, refreshed_name: str | None) -> bool:
    if not existing_name or not refreshed_name:
        return True

    existing_slug = slugify(existing_name)
    refreshed_slug = slugify(refreshed_name)
    if existing_slug == refreshed_slug:
        return True

    if existing_slug in refreshed_slug or refreshed_slug in existing_slug:
        shorter = min(len(existing_slug), len(refreshed_slug))
        return shorter >= 8

    return False


def raw_place_address_is_preservable(existing_place: RawPlace, refreshed_place: RawPlace) -> bool:
    address = as_string(existing_place.address)
    if not address:
        return False

    note = as_string(existing_place.note)
    if note and normalize_text(note) == normalize_text(address):
        return False

    existing_name = as_string(existing_place.name)
    refreshed_name = as_string(refreshed_place.name)
    normalized_address = normalize_text(address)
    if normalized_address is None:
        return False
    if existing_name and normalize_text(existing_name) == normalized_address:
        return False
    if refreshed_name and normalize_text(refreshed_name) == normalized_address:
        return False

    # Preserve only address-like strings, not generic locality labels or notes.
    return (
        bool(re.search(r"\d", address))
        or "," in address
        or "、" in address
        or bool(re.search(r"\b(?:street|st|avenue|ave|road|rd|boulevard|blvd|lane|ln|drive|dr|way|place|pl|court|ct|suite|ste|unit|floor|fl)\b", address, re.IGNORECASE))
        or "〒" in address
        or "號" in address
    )


def preserve_existing_raw_saved_list(
    *,
    source: SourceConfig | None = None,
    slug: str,
    existing_payload: RawSavedList | None,
    refreshed_payload: RawSavedList,
) -> RawSavedList:
    if existing_payload is None:
        return refreshed_payload
    if source is not None:
        if not raw_source_signature_matches(source, existing_payload.source_signature):
            return refreshed_payload
    elif (
        existing_payload.source_signature
        and refreshed_payload.source_signature
        and existing_payload.source_signature != refreshed_payload.source_signature
    ):
        return refreshed_payload

    source_type = refreshed_payload.configured_source_type or existing_payload.configured_source_type
    existing_index = build_raw_place_preservation_index(existing_payload, source_type=source_type)
    updated_places: list[RawPlace] = []

    for refreshed_place in refreshed_payload.places:
        existing_place = None
        for key in raw_place_match_keys(refreshed_place, source_type=source_type):
            existing_place = existing_index.get(key)
            if existing_place is not None:
                break

        if existing_place is None:
            updated_places.append(refreshed_place)
            continue

        merged_place, preserved_fields = preserve_existing_raw_place(
            existing_place=existing_place,
            refreshed_place=refreshed_place,
        )
        if preserved_fields:
            place_id = stable_place_id(merged_place, source_type=source_type)
            print(
                f"WARNING: Preserving previous raw fields for {slug}:{place_id} "
                f"[{merged_place.name}]: {', '.join(preserved_fields)}.",
                flush=True,
            )
        updated_places.append(merged_place)

    return refreshed_payload.model_copy(update={"places": updated_places})


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


ENRICHMENT_REFRESH_REASON_PRIORITY = {
    "missing-cache-entry": 0,
    "raw-place-changed": 1,
    "invalid-refresh-after": 2,
    "invalid-fetched-at": 2,
    "missing-input-signature": 2,
    "legacy-cache-entry": 3,
    "refresh-window-expired": 4,
    "forced": 5,
}


def enrichment_refresh_reason(
    place: RawPlace,
    cache_entry: EnrichmentCacheEntry | None,
    *,
    force_refresh: bool,
    missing_only: bool,
) -> str | None:
    if force_refresh:
        return "forced"

    refresh_reason = cache_refresh_reason(place, cache_entry)
    if missing_only and refresh_reason != "missing-cache-entry":
        return None
    return refresh_reason


def enrichment_job_priority(job: tuple[str, str, str, str, dict[str, Any]]) -> tuple[int, str, str]:
    slug, place_id, _place_name, refresh_reason, _place_payload = job
    return (
        ENRICHMENT_REFRESH_REASON_PRIORITY.get(refresh_reason, 99),
        slug,
        place_id,
    )
def place_input_signature(place: RawPlace) -> str:
    payload = structured_place_identity_payload(place)
    if payload is None:
        payload = {
            "name": place.name,
            "address": place.address,
            "lat": place.lat,
            "lng": place.lng,
        }
    serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def structured_place_identity_payload(place: RawPlace) -> dict[str, str] | None:
    cid = place.cid or extract_maps_cid(place.maps_url)
    if cid:
        return {"cid": cid}

    google_id = as_string(place.google_id)
    if google_id:
        return {"google_id": google_id.strip("/").replace("/", "-")}

    maps_place_token = place.maps_place_token or extract_maps_place_token(place.maps_url)
    if maps_place_token:
        return {"maps_place_token": maps_place_token}

    return None


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


def cache_entry_has_publishable_enrichment(entry: EnrichmentCacheEntry | None) -> bool:
    return bool(entry and entry.error is None and entry.matched is True and entry.place is not None)


def append_unique_reason(reasons: list[str], reason: str) -> None:
    if reason not in reasons:
        reasons.append(reason)


def google_maps_uri_strength(url: str | None) -> int:
    normalized = as_string(url)
    if normalized is None:
        return 0
    lowered = normalized.lower()
    if "query_place_id=" in lowered:
        return 4
    if "/maps/place/" in lowered or re.search(r"[?&]cid=[^&#]+", lowered):
        return 3
    if "/maps/search/" in lowered:
        return 1
    return 2


def preserve_existing_enrichment(
    *,
    slug: str,
    place_id: str,
    place_name: str,
    existing_entry: EnrichmentCacheEntry | None,
    refreshed_entry: EnrichmentCacheEntry,
) -> tuple[EnrichmentCacheEntry, str | None]:
    if not cache_entry_has_publishable_enrichment(existing_entry):
        return refreshed_entry, None

    if not cache_entry_has_publishable_enrichment(refreshed_entry):
        reason = refreshed_entry.error or "no matched enrichment"
        return (
            existing_entry,
            f"WARNING: Preserving previous enrichment for {slug}:{place_id} [{place_name}] "
            f"because refresh returned degraded result ({reason}).",
        )

    previous_place = existing_entry.place
    refreshed_place = refreshed_entry.place
    canonicalize_enrichment_place(previous_place)
    canonicalize_enrichment_place(refreshed_place)
    assert previous_place is not None
    assert refreshed_place is not None

    preserved_fields: list[str] = []

    if refreshed_place.rating is None and previous_place.rating is not None:
        refreshed_place.rating = previous_place.rating
        append_unique_reason(preserved_fields, "rating")
    if refreshed_place.user_rating_count is None and previous_place.user_rating_count is not None:
        refreshed_place.user_rating_count = previous_place.user_rating_count
        append_unique_reason(preserved_fields, "user_rating_count")

    if not refreshed_place.primary_type_display_name and previous_place.primary_type_display_name:
        refreshed_place.primary_type_display_name = previous_place.primary_type_display_name
        if not refreshed_place.primary_type and previous_place.primary_type:
            refreshed_place.primary_type = previous_place.primary_type
        if not refreshed_place.types and previous_place.types:
            refreshed_place.types = previous_place.types[:]
        append_unique_reason(preserved_fields, "primary_category")
    elif (
        refreshed_place.primary_type_display_name == previous_place.primary_type_display_name
        and not refreshed_place.primary_type
        and previous_place.primary_type
    ):
        refreshed_place.primary_type = previous_place.primary_type
        if not refreshed_place.types and previous_place.types:
            refreshed_place.types = previous_place.types[:]

    if not refreshed_place.primary_type_display_name_localized and previous_place.primary_type_display_name_localized:
        refreshed_place.primary_type_display_name_localized = previous_place.primary_type_display_name_localized

    if not refreshed_place.business_status and previous_place.business_status:
        refreshed_place.business_status = previous_place.business_status
        append_unique_reason(preserved_fields, "status")

    if google_maps_uri_strength(refreshed_place.google_maps_uri) < google_maps_uri_strength(previous_place.google_maps_uri):
        refreshed_place.google_maps_uri = previous_place.google_maps_uri
        if not refreshed_place.google_place_id and previous_place.google_place_id:
            refreshed_place.google_place_id = previous_place.google_place_id
        if not refreshed_place.google_place_resource_name and previous_place.google_place_resource_name:
            refreshed_place.google_place_resource_name = previous_place.google_place_resource_name
        append_unique_reason(preserved_fields, "maps_url")
    else:
        if not refreshed_place.google_place_id and previous_place.google_place_id:
            refreshed_place.google_place_id = previous_place.google_place_id
        if not refreshed_place.google_place_resource_name and previous_place.google_place_resource_name:
            refreshed_place.google_place_resource_name = previous_place.google_place_resource_name

    if not refreshed_place.main_photo_url and previous_place.main_photo_url:
        refreshed_place.main_photo_url = previous_place.main_photo_url
        append_unique_reason(preserved_fields, "photo_url")
    if not refreshed_place.photo_url and previous_place.photo_url:
        refreshed_place.photo_url = previous_place.photo_url
        append_unique_reason(preserved_fields, "photo_url")

    if not preserved_fields:
        return refreshed_entry, None

    return (
        refreshed_entry,
        f"WARNING: Preserving previous enrichment fields for {slug}:{place_id} [{place_name}]: "
        f"{', '.join(preserved_fields)}.",
    )


def load_places_cache(slug: str) -> dict[str, EnrichmentCacheEntry]:
    return load_places_cache_from_sqlite(slug) or {}


def save_places_cache(slug: str, payload: dict[str, EnrichmentCacheEntry]) -> None:
    save_places_cache_to_sqlite(slug, payload)


def export_places_cache_json(slug: str, payload: dict[str, EnrichmentCacheEntry]) -> None:
    write_json(PLACES_CACHE_DIR / f"{slug}.json", payload)


def sqlite_table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def sqlite_bool_or_none(value: bool | None) -> int | None:
    if value is None:
        return None
    return sqlite_bool(value)


def sqlite_error_is_unreadable_database(error: sqlite3.Error) -> bool:
    message = str(error).lower()
    return any(marker in message for marker in SQLITE_UNREADABLE_ERROR_MARKERS)


def ensure_guide_enrichment_cache_table(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS guide_enrichment_cache (
            guide_slug TEXT NOT NULL,
            place_id TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            last_verified_at TEXT,
            refresh_after TEXT,
            source TEXT,
            query TEXT NOT NULL,
            input_signature TEXT,
            matched INTEGER,
            score INTEGER,
            error TEXT,
            error_body TEXT,
            cache_json TEXT NOT NULL,
            PRIMARY KEY (guide_slug, place_id)
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_guide_enrichment_cache_place_id
            ON guide_enrichment_cache(place_id)
        """
    )


def load_places_cache_from_sqlite(slug: str) -> dict[str, EnrichmentCacheEntry] | None:
    if not PLACES_SQLITE_PATH.exists():
        return None

    connection = sqlite3.connect(PLACES_SQLITE_PATH)
    try:
        try:
            if not sqlite_table_exists(connection, "guide_enrichment_cache"):
                return None

            rows = connection.execute(
                """
                SELECT place_id, cache_json
                FROM guide_enrichment_cache
                WHERE guide_slug = ?
                ORDER BY place_id
                """,
                (slug,),
            ).fetchall()
        except sqlite3.DatabaseError as error:
            if not sqlite_error_is_unreadable_database(error):
                raise
            return None
    finally:
        connection.close()

    result: dict[str, EnrichmentCacheEntry] = {}
    for place_id, cache_json in rows:
        if not isinstance(place_id, str) or not isinstance(cache_json, str):
            continue
        result[place_id] = EnrichmentCacheEntry.model_validate_json(cache_json)
    return result


def save_places_cache_to_sqlite(slug: str, payload: dict[str, EnrichmentCacheEntry]) -> None:
    PLACES_SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(PLACES_SQLITE_PATH)
    try:
        with connection:
            ensure_guide_enrichment_cache_table(connection)
            connection.execute(
                "DELETE FROM guide_enrichment_cache WHERE guide_slug = ?",
                (slug,),
            )
            if payload:
                connection.executemany(
                    """
                    INSERT INTO guide_enrichment_cache (
                        guide_slug, place_id, fetched_at, last_verified_at, refresh_after, source,
                        query, input_signature, matched, score, error, error_body, cache_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            slug,
                            place_id,
                            entry.fetched_at,
                            entry.last_verified_at,
                            entry.refresh_after,
                            entry.source,
                            entry.query,
                            entry.input_signature,
                            sqlite_bool_or_none(entry.matched),
                            entry.score,
                            entry.error,
                            entry.error_body,
                            json.dumps(entry.model_dump(mode="json"), ensure_ascii=False, separators=(",", ":")),
                        )
                        for place_id, entry in sorted(payload.items())
                    ],
                )
    finally:
        connection.close()


def export_all_places_cache_json() -> int:
    PLACES_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    exported = 0
    for path in sorted(PLACES_CACHE_DIR.glob("*.json")):
        path.unlink()

    for raw_path in sorted(RAW_DIR.glob("*.json")):
        slug = raw_path.stem
        payload = load_places_cache(slug)
        if not payload:
            continue
        export_places_cache_json(slug, payload)
        exported += 1

    return exported


def migrate_cache_to_sqlite(*, rewrite_raw_maps_urls: bool = True) -> tuple[int, int, int, int]:
    sync_local_csv_sources()
    migrated_guides = 0
    raw_places_rewritten = 0
    cache_entries_rewritten = 0
    sqlite_cache_rows = 0

    for raw_path in sorted(RAW_DIR.glob("*.json")):
        slug = raw_path.stem
        raw = RawSavedList.model_validate_json(raw_path.read_text(encoding="utf-8"))
        raw_changed = False
        cache_changed = False
        updated_places: list[RawPlace] = []

        cache_payload = load_places_cache_json(slug)
        if not cache_payload:
            cache_payload = load_places_cache_from_sqlite(slug) or {}
        cache_migration_index = build_cache_migration_index(cache_payload)

        for place in raw.places:
            updated_place = place
            if rewrite_raw_maps_urls:
                public_maps_url = build_public_google_maps_url(
                    name=place.name,
                    address=place.address,
                    lat=place.lat,
                    lng=place.lng,
                    raw_maps_url=place.maps_url,
                )
                if public_maps_url != place.maps_url:
                    updated_place = updated_place.model_copy(update={"maps_url": public_maps_url})
                    raw_changed = True
                    raw_places_rewritten += 1

            place_id = stable_place_id(updated_place, source_type=raw.configured_source_type)
            cache_entry, migrated = migrate_cache_entry_for_place(
                cache_payload,
                cache_migration_index,
                place=updated_place,
                place_id=place_id,
            )
            if migrated:
                cache_changed = True
                cache_entries_rewritten += 1
            if cache_entry is not None:
                normalized_entry = migrate_cache_entry(cache_entry, updated_place)
                if normalized_entry.model_dump(mode="json") != cache_entry.model_dump(mode="json"):
                    cache_payload[place_id] = normalized_entry
                    cache_changed = True
                    cache_entries_rewritten += 1

            updated_places.append(updated_place)

        if raw_changed:
            raw = raw.model_copy(update={"places": updated_places})
            write_json(raw_path, raw)

        save_places_cache(slug, cache_payload)
        sqlite_cache_rows += len(cache_payload)
        if raw_changed or cache_changed:
            migrated_guides += 1

    return migrated_guides, raw_places_rewritten, cache_entries_rewritten, sqlite_cache_rows


def place_photo_signature_state(
    public_path: str | None,
    *,
    source_photo_url: str | None,
    metadata_cache: dict[str, tuple[Any, ...]],
) -> tuple[Any, ...]:
    cache_key = public_path or f"missing:{source_photo_url or 'none'}"
    cached = metadata_cache.get(cache_key)
    if cached is not None:
        return cached

    absolute_path = public_asset_absolute_path(public_path)
    if absolute_path and absolute_path.exists():
        stat_result = absolute_path.stat()
        state = (
            "downloaded",
            sha256_file(absolute_path),
            stat_result.st_size,
            source_photo_url,
            public_path,
        )
    elif source_photo_url:
        state = ("missing_file", source_photo_url, public_path)
    else:
        state = ("missing_url", public_path)

    metadata_cache[cache_key] = state
    return state


def build_places_sqlite_signature(
    *,
    raw_lists: dict[str, RawSavedList],
    guides: list[Guide],
    enrichment_caches: dict[str, dict[str, EnrichmentCacheEntry]],
) -> str:
    photo_state_cache: dict[str, tuple[Any, ...]] = {}
    photo_states: list[dict[str, Any]] = []

    for guide in sorted(guides, key=lambda item: item.slug):
        enrichment_cache = enrichment_caches.get(guide.slug, {})
        for sort_order, place in enumerate(guide.places):
            cache_entry = enrichment_cache.get(place.id)
            source_photo_url = cached_place_photo_url(cache_entry)
            if source_photo_url or place.main_photo_path:
                photo_states.append(
                    {
                        "guide_slug": guide.slug,
                        "sort_order": sort_order,
                        "place_id": place.id,
                        "state": place_photo_signature_state(
                            place.main_photo_path,
                            source_photo_url=source_photo_url,
                            metadata_cache=photo_state_cache,
                        ),
                    }
                )

    payload = {
        "signature_version": PLACES_SQLITE_SIGNATURE_VERSION,
        "schema_sha256": hashlib.sha256(PLACES_SQLITE_SCHEMA_SQL.encode("utf-8")).hexdigest(),
        "raw_lists": {
            slug: raw.model_dump(mode="json")
            for slug, raw in sorted(raw_lists.items())
        },
        "guides": [
            guide.model_dump(mode="json")
            for guide in sorted(guides, key=lambda item: item.slug)
        ],
        "enrichment_caches": {
            slug: {
                place_id: entry.model_dump(mode="json")
                for place_id, entry in sorted(cache_payload.items())
            }
            for slug, cache_payload in sorted(enrichment_caches.items())
        },
        "photo_states": photo_states,
    }
    serialized = json.dumps(
        normalize_json_payload(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def load_places_sqlite_build_signature() -> str | None:
    if not PLACES_SQLITE_PATH.exists():
        return None

    connection = sqlite3.connect(PLACES_SQLITE_PATH)
    try:
        try:
            if not sqlite_table_exists(connection, "build_metadata"):
                return None
            row = connection.execute(
                "SELECT value FROM build_metadata WHERE key = ?",
                (PLACES_SQLITE_BUILD_METADATA_KEY,),
            ).fetchone()
        except sqlite3.DatabaseError as error:
            if not sqlite_error_is_unreadable_database(error):
                raise
            return None
    finally:
        connection.close()

    if row is None or not isinstance(row[0], str):
        return None
    return row[0]


def build_places_sqlite_rows(
    *,
    raw_lists: dict[str, RawSavedList],
    guides: list[Guide],
    enrichment_caches: dict[str, dict[str, EnrichmentCacheEntry]],
) -> PlacesSqliteRows:
    raw_place_maps = {
        slug: {
            stable_place_id(place, source_type=raw.configured_source_type): place
            for place in raw.places
        }
        for slug, raw in raw_lists.items()
    }
    photo_metadata_cache: dict[str, tuple[str, str | None, str | None, int | None]] = {}
    canonical_places: dict[str, tuple[tuple[int, int, int, int], tuple[Any, ...]]] = {}
    guide_rows: list[tuple[Any, ...]] = []
    guide_place_rows: list[tuple[Any, ...]] = []
    photo_rows: list[tuple[Any, ...]] = []

    for guide in guides:
        guide_rows.append(
            (
                guide.slug,
                guide.title,
                guide.description,
                guide.source_url,
                guide.list_id,
                guide.country_name,
                guide.country_code,
                guide.city_name,
                sqlite_json(guide.list_tags),
                sqlite_json(guide.featured_place_ids),
                sqlite_json(guide.best_hit_place_ids),
                guide.best_hit_min_rating,
                guide.best_hit_min_reviews,
                sqlite_json(guide.top_categories),
                guide.generated_at,
                guide.place_count,
                guide.center_lat,
                guide.center_lng,
            )
        )

        raw_places_by_id = raw_place_maps.get(guide.slug, {})
        enrichment_cache = enrichment_caches.get(guide.slug, {})
        featured_ids = set(guide.featured_place_ids)
        best_hit_ids = set(guide.best_hit_place_ids)

        for sort_order, place in enumerate(guide.places):
            raw_place = raw_places_by_id.get(place.id)
            cache_entry = enrichment_cache.get(place.id)
            candidate_row = canonical_place_row(
                guide_slug=guide.slug,
                raw_place=raw_place,
                place=place,
                cache_entry=cache_entry,
            )
            candidate_score = canonical_place_score(place, cache_entry)
            current = canonical_places.get(place.id)
            if current is None or candidate_score > current[0]:
                canonical_places[place.id] = (candidate_score, candidate_row)

            guide_place_rows.append(
                (
                    guide.slug,
                    sort_order,
                    place.id,
                    sqlite_bool(place.id in featured_ids),
                    sqlite_bool(place.id in best_hit_ids),
                    place.name,
                    place.neighborhood,
                    place.primary_category,
                    place.primary_category_localized,
                    place.rating,
                    place.user_rating_count,
                    place.status,
                    sqlite_bool(place.top_pick),
                    sqlite_bool(place.hidden),
                    place.manual_rank,
                    place.note,
                    place.why_recommended,
                    place.main_photo_path,
                    place.maps_url,
                )
            )

            source_photo_url = cached_place_photo_url(cache_entry)
            if source_photo_url or place.main_photo_path:
                fetch_status, last_fetched_at, content_sha256, size_bytes = place_photo_metadata(
                    place.main_photo_path,
                    source_photo_url=source_photo_url,
                    metadata_cache=photo_metadata_cache,
                )
                photo_rows.append(
                    (
                        guide.slug,
                        sort_order,
                        place.id,
                        source_photo_url,
                        place.main_photo_path,
                        fetch_status,
                        last_fetched_at,
                        content_sha256,
                        size_bytes,
                    )
                )

    guide_cache_rows = [
        (
            guide_slug,
            place_id,
            entry.fetched_at,
            entry.last_verified_at,
            entry.refresh_after,
            entry.source,
            entry.query,
            entry.input_signature,
            sqlite_bool_or_none(entry.matched),
            entry.score,
            entry.error,
            entry.error_body,
            json.dumps(entry.model_dump(mode="json"), ensure_ascii=False, separators=(",", ":")),
        )
        for guide_slug, payload in sorted(enrichment_caches.items())
        for place_id, entry in sorted(payload.items())
    ]

    return PlacesSqliteRows(
        guide_rows=guide_rows,
        canonical_place_rows=[
            row
            for _, row in (
                canonical_places[place_id]
                for place_id in sorted(canonical_places)
            )
        ],
        guide_cache_rows=guide_cache_rows,
        guide_place_rows=guide_place_rows,
        photo_rows=photo_rows,
    )


def write_places_sqlite(
    rows: PlacesSqliteRows,
    *,
    build_signature: str,
) -> None:
    PLACES_SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = PLACES_SQLITE_PATH.with_suffix(".sqlite.tmp")
    tmp_path.unlink(missing_ok=True)

    connection = sqlite3.connect(tmp_path)
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = DELETE")
        connection.execute("PRAGMA synchronous = NORMAL")
        connection.executescript(PLACES_SQLITE_SCHEMA_SQL)

        canonical_place_columns = """
            place_id, first_seen_guide_slug, raw_name, raw_address, raw_lat, raw_lng, raw_maps_url,
            raw_cid, raw_google_id, raw_maps_place_token, normalized_name,
            normalized_address, normalized_lat, normalized_lng, normalized_maps_url, normalized_cid,
            normalized_google_id, normalized_google_place_id, normalized_google_place_resource_name,
            normalized_rating, normalized_user_rating_count, normalized_primary_category,
            normalized_primary_category_localized,
            normalized_tags_json, normalized_vibe_tags_json, normalized_neighborhood, normalized_note,
            normalized_why_recommended, normalized_main_photo_path, normalized_top_pick,
            normalized_hidden, normalized_manual_rank, normalized_status, cache_fetched_at,
            cache_last_verified_at, cache_refresh_after, cache_source, cache_query, cache_input_signature,
            cache_matched, cache_score, cache_error, cache_error_body,
            enrichment_google_place_id, enrichment_google_place_resource_name, enrichment_display_name,
            enrichment_formatted_address, enrichment_google_maps_uri, enrichment_rating,
            enrichment_user_rating_count, enrichment_primary_type, enrichment_primary_type_display_name,
            enrichment_primary_type_display_name_localized,
            enrichment_types_json, enrichment_business_status, enrichment_website, enrichment_phone,
            enrichment_plus_code, enrichment_description, enrichment_main_photo_url, enrichment_photo_url,
            enrichment_limited_view
        """

        with connection:
            connection.execute(
                "INSERT INTO build_metadata (key, value) VALUES (?, ?)",
                (PLACES_SQLITE_BUILD_METADATA_KEY, build_signature),
            )
            connection.executemany(
                """
                INSERT INTO guides (
                    slug, title, description, source_url, list_id, country_name, country_code, city_name,
                    list_tags_json, featured_place_ids_json, best_hit_place_ids_json, best_hit_min_rating,
                    best_hit_min_reviews, top_categories_json, generated_at, place_count, center_lat, center_lng
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows.guide_rows,
            )
            for canonical_row in rows.canonical_place_rows:
                placeholders = ", ".join("?" for _ in canonical_row)
                connection.execute(
                    """
                    INSERT INTO canonical_places ({columns})
                    VALUES ({placeholders})
                    """.format(columns=canonical_place_columns, placeholders=placeholders),
                    canonical_row,
                )
            connection.executemany(
                """
                INSERT INTO guide_enrichment_cache (
                    guide_slug, place_id, fetched_at, last_verified_at, refresh_after, source,
                    query, input_signature, matched, score, error, error_body, cache_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows.guide_cache_rows,
            )
            connection.executemany(
                """
                INSERT INTO guide_places (
                    guide_slug, sort_order, place_id, is_featured, is_best_hit, place_name,
                    neighborhood, primary_category, primary_category_localized, rating, user_rating_count,
                    status, top_pick, hidden, manual_rank, note, why_recommended, main_photo_path, maps_url
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows.guide_place_rows,
            )
            connection.executemany(
                """
                INSERT INTO place_photos (
                    guide_slug, sort_order, place_id, source_photo_url, local_path, fetch_status,
                    last_fetched_at, content_sha256, size_bytes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows.photo_rows,
            )
    finally:
        connection.close()

    tmp_path.replace(PLACES_SQLITE_PATH)


def rebuild_places_sqlite(
    *,
    raw_lists: dict[str, RawSavedList],
    guides: list[Guide],
    enrichment_caches: dict[str, dict[str, EnrichmentCacheEntry]],
) -> None:
    build_signature = build_places_sqlite_signature(
        raw_lists=raw_lists,
        guides=guides,
        enrichment_caches=enrichment_caches,
    )
    if load_places_sqlite_build_signature() == build_signature:
        return

    rows = build_places_sqlite_rows(
        raw_lists=raw_lists,
        guides=guides,
        enrichment_caches=enrichment_caches,
    )
    write_places_sqlite(rows, build_signature=build_signature)


def canonical_place_score(
    place: NormalizedPlace,
    cache_entry: EnrichmentCacheEntry | None,
) -> tuple[int, int, int, int]:
    enrichment_place = cache_entry.place if cache_entry else None
    return (
        1 if enrichment_place is not None else 0,
        1 if place.main_photo_path else 0,
        place.user_rating_count or -1,
        sum(
            1
            for value in (
                place.address,
                place.neighborhood,
                place.note,
                place.why_recommended,
                enrichment_place.display_name if enrichment_place else None,
                enrichment_place.formatted_address if enrichment_place else None,
                enrichment_place.website if enrichment_place else None,
                enrichment_place.phone if enrichment_place else None,
            )
            if value
        ),
    )


def canonical_place_row(
    *,
    guide_slug: str,
    raw_place: RawPlace | None,
    place: NormalizedPlace,
    cache_entry: EnrichmentCacheEntry | None,
) -> tuple[Any, ...]:
    enrichment_place = cache_entry.place if cache_entry and cache_entry.place else None
    return (
        place.id,
        guide_slug,
        raw_place.name if raw_place else None,
        raw_place.address if raw_place else None,
        raw_place.lat if raw_place else None,
        raw_place.lng if raw_place else None,
        raw_place.maps_url if raw_place else None,
        raw_place.cid if raw_place else None,
        raw_place.google_id if raw_place else None,
        raw_place.maps_place_token if raw_place else None,
        place.name,
        place.address,
        place.lat,
        place.lng,
        place.maps_url,
        place.cid,
        place.google_id,
        place.google_place_id,
        place.google_place_resource_name,
        place.rating,
        place.user_rating_count,
        place.primary_category,
        place.primary_category_localized,
        sqlite_json(place.tags),
        sqlite_json(place.vibe_tags),
        place.neighborhood,
        place.note,
        place.why_recommended,
        place.main_photo_path,
        sqlite_bool(place.top_pick),
        sqlite_bool(place.hidden),
        place.manual_rank,
        place.status,
        cache_entry.fetched_at if cache_entry else None,
        cache_entry.last_verified_at if cache_entry else None,
        cache_entry.refresh_after if cache_entry else None,
        cache_entry.source if cache_entry else None,
        cache_entry.query if cache_entry else None,
        cache_entry.input_signature if cache_entry else None,
        sqlite_bool(cache_entry.matched) if cache_entry and cache_entry.matched is not None else None,
        cache_entry.score if cache_entry else None,
        cache_entry.error if cache_entry else None,
        cache_entry.error_body if cache_entry else None,
        enrichment_place.google_place_id if enrichment_place else None,
        enrichment_place.google_place_resource_name if enrichment_place else None,
        enrichment_place.display_name if enrichment_place else None,
        enrichment_place.formatted_address if enrichment_place else None,
        enrichment_place.google_maps_uri if enrichment_place else None,
        enrichment_place.rating if enrichment_place else None,
        enrichment_place.user_rating_count if enrichment_place else None,
        enrichment_place.primary_type if enrichment_place else None,
        enrichment_place.primary_type_display_name if enrichment_place else None,
        enrichment_place.primary_type_display_name_localized if enrichment_place else None,
        sqlite_json(enrichment_place.types if enrichment_place else []),
        enrichment_place.business_status if enrichment_place else None,
        enrichment_place.website if enrichment_place else None,
        enrichment_place.phone if enrichment_place else None,
        enrichment_place.plus_code if enrichment_place else None,
        enrichment_place.description if enrichment_place else None,
        enrichment_place.main_photo_url if enrichment_place else None,
        enrichment_place.photo_url if enrichment_place else None,
        sqlite_bool(enrichment_place.limited_view) if enrichment_place else None,
    )


def sqlite_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def sqlite_bool(value: bool) -> int:
    return 1 if value else 0


def place_photo_metadata(
    public_path: str | None,
    *,
    source_photo_url: str | None,
    metadata_cache: dict[str, tuple[str, str | None, str | None, int | None]],
) -> tuple[str, str | None, str | None, int | None]:
    cache_key = public_path or f"missing:{source_photo_url or 'none'}"
    cached = metadata_cache.get(cache_key)
    if cached is not None:
        return cached

    absolute_path = public_asset_absolute_path(public_path)
    if absolute_path and absolute_path.exists():
        stat_result = absolute_path.stat()
        metadata = (
            "downloaded",
            datetime.fromtimestamp(stat_result.st_mtime, tz=UTC).isoformat(),
            sha256_file(absolute_path),
            stat_result.st_size,
        )
    elif source_photo_url:
        metadata = ("missing_file", None, None, None)
    else:
        metadata = ("missing_url", None, None, None)

    metadata_cache[cache_key] = metadata
    return metadata


def public_asset_absolute_path(public_path: str | None) -> Path | None:
    normalized = as_string(public_path)
    if normalized is None:
        return None
    relative_path = normalized.lstrip("/")
    site_public_path = SITE_DIR / "public" / relative_path
    if site_public_path.exists():
        return site_public_path
    return ROOT / "public" / relative_path


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sync_guide_place_photos(
    guide: Guide,
    *,
    enrichment_cache: dict[str, EnrichmentCacheEntry],
    startup_jitter_seconds: float = 0,
) -> None:
    for place in guide.places:
        photo_url = cached_place_photo_url(enrichment_cache.get(place.id))
        place.main_photo_path = sync_place_photo(
            guide.slug,
            place.id,
            photo_url=photo_url,
            startup_jitter_seconds=startup_jitter_seconds,
        )


def cached_place_photo_url(cache_entry: EnrichmentCacheEntry | None) -> str | None:
    if cache_entry is None or cache_entry.place is None:
        return None
    return cache_entry.place.main_photo_url or cache_entry.place.photo_url


def populate_place_photos_for_guides(
    guides: list[Guide],
    *,
    enrichment_caches: dict[str, dict[str, EnrichmentCacheEntry]],
    refresh_photos: bool,
    photo_workers: int,
    startup_jitter_seconds: float,
) -> None:
    if not refresh_photos:
        for guide in guides:
            enrichment_cache = enrichment_caches.get(guide.slug, {})
            for place in guide.places:
                photo_url = cached_place_photo_url(enrichment_cache.get(place.id))
                place.main_photo_path, fallback_reason = resolve_existing_place_photo_path(
                    guide.slug,
                    place.id,
                    photo_url=photo_url,
                    allow_stale_place_id_fallback=True,
                )
                if fallback_reason == "missing-photo-url":
                    print(
                        f"WARNING: Reusing existing local photo for {guide.slug}:{place.id} "
                        "because current enrichment did not yield a photo URL. "
                        "Photo extraction may be failing on this runner.",
                        flush=True,
                    )
        return

    pending_jobs: list[PendingPhotoJob] = []
    reused_count = 0
    missing_photo_url_count = 0
    for guide in guides:
        enrichment_cache = enrichment_caches.get(guide.slug, {})
        for place in guide.places:
            photo_url = cached_place_photo_url(enrichment_cache.get(place.id))
            if not photo_url:
                place.main_photo_path = None
                missing_photo_url_count += 1
                continue

            existing_path = existing_place_photo_path(
                guide.slug,
                place.id,
                photo_url=photo_url,
            )
            if existing_path is not None:
                place.main_photo_path = existing_path
                reused_count += 1
                continue

            pending_jobs.append(
                PendingPhotoJob(
                    guide_slug=guide.slug,
                    place_id=place.id,
                    place_name=place.name,
                    photo_url=photo_url,
                )
            )

    if not pending_jobs:
        print(
            "No new place photos to download"
            f" ({reused_count} existing, {missing_photo_url_count} without photo URLs)",
            flush=True,
        )
        return

    place_by_key = {
        (guide.slug, place.id): place
        for guide in guides
        for place in guide.places
    }

    max_workers = max(1, min(photo_workers, len(pending_jobs)))
    effective_startup_jitter_seconds = startup_jitter_seconds if len(pending_jobs) > 1 else 0
    print(
        "Downloading"
        f" {len(pending_jobs)} place photos with {max_workers} workers"
        f" ({reused_count} existing, {missing_photo_url_count} without photo URLs)",
        flush=True,
    )

    if max_workers == 1 or len(pending_jobs) == 1:
        for index, job in enumerate(pending_jobs, start=1):
            photo_path = sync_place_photo(
                job.guide_slug,
                job.place_id,
                photo_url=job.photo_url,
                startup_jitter_seconds=effective_startup_jitter_seconds,
            )
            place_by_key[(job.guide_slug, job.place_id)].main_photo_path = photo_path
            print(photo_progress_line(index, len(pending_jobs), job, photo_path), flush=True)
        return

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(
                sync_place_photo,
                job.guide_slug,
                job.place_id,
                photo_url=job.photo_url,
                startup_jitter_seconds=effective_startup_jitter_seconds,
            ): job
            for job in pending_jobs
        }
        for index, future in enumerate(as_completed(future_map), start=1):
            job = future_map[future]
            photo_path = future.result()
            place_by_key[(job.guide_slug, job.place_id)].main_photo_path = photo_path
            print(photo_progress_line(index, len(pending_jobs), job, photo_path), flush=True)


def photo_progress_line(
    completed_count: int,
    total_count: int,
    job: PendingPhotoJob,
    photo_path: str | None,
) -> str:
    outcome = "downloaded" if photo_path else "failed"
    return (
        f"[photos {completed_count}/{total_count}] {outcome}: "
        f"{job.guide_slug} / {job.place_name}"
    )


def resolve_existing_place_photo_path(
    slug: str,
    place_id: str,
    *,
    photo_url: str | None,
    allow_stale_place_id_fallback: bool = False,
) -> tuple[str | None, str | None]:
    if photo_url:
        filename_glob = canonical_place_photo_glob(place_id, photo_url)
        canonical_matches = sorted(PLACE_PHOTOS_DIR.glob(filename_glob))
        if canonical_matches:
            remove_legacy_place_photo_matches(slug, filename_glob=filename_glob)
            return public_photo_path(canonical_matches[0].name), None

        legacy_matches = sorted((PLACE_PHOTOS_DIR / slug).glob(filename_glob))
        if legacy_matches:
            migrated_path = migrate_legacy_place_photo_to_flat_dir(
                slug,
                filename_glob=filename_glob,
                legacy_path=legacy_matches[0],
            )
            return public_photo_path(migrated_path.name), None

    if not allow_stale_place_id_fallback:
        return None, None

    place_prefix = f"{safe_place_photo_stem(place_id)}-"
    fallback_matches = sorted(
        (
            path
            for path in PLACE_PHOTOS_DIR.glob(f"{place_prefix}*.*")
            if path.is_file() and not path.name.startswith(".")
        ),
        key=lambda path: (path.stat().st_mtime, path.name),
        reverse=True,
    )
    if fallback_matches:
        fallback_reason = "stale-photo-url" if photo_url else "missing-photo-url"
        return public_photo_path(fallback_matches[0].name), fallback_reason

    legacy_dir = PLACE_PHOTOS_DIR / slug
    legacy_fallback_matches = sorted(
        (
            path
            for path in legacy_dir.glob(f"{place_prefix}*.*")
            if path.is_file() and not path.name.startswith(".")
        ),
        key=lambda path: (path.stat().st_mtime, path.name),
        reverse=True,
    )
    if not legacy_fallback_matches:
        return None, None

    migrated_path = migrate_legacy_place_photo_to_flat_dir(
        slug,
        filename_glob=legacy_fallback_matches[0].name,
        legacy_path=legacy_fallback_matches[0],
    )
    fallback_reason = "stale-photo-url" if photo_url else "missing-photo-url"
    return public_photo_path(migrated_path.name), fallback_reason


def existing_place_photo_path(
    slug: str,
    place_id: str,
    *,
    photo_url: str | None,
    allow_stale_place_id_fallback: bool = False,
) -> str | None:
    path, _fallback_reason = resolve_existing_place_photo_path(
        slug,
        place_id,
        photo_url=photo_url,
        allow_stale_place_id_fallback=allow_stale_place_id_fallback,
    )
    return path


def sync_place_photo(
    slug: str,
    place_id: str,
    *,
    photo_url: str | None,
    startup_jitter_seconds: float = 0,
) -> str | None:
    if not photo_url:
        return None

    photo_dir = PLACE_PHOTOS_DIR
    photo_dir.mkdir(parents=True, exist_ok=True)

    existing_path = existing_place_photo_path(slug, place_id, photo_url=photo_url)
    if existing_path is not None:
        return existing_path

    try:
        sleep_for_refresh_startup_jitter(startup_jitter_seconds)
        request = Request(
            photo_url,
            headers={"User-Agent": "Mozilla/5.0 favorite-places photo fetch"},
        )
        with urlopen(request, timeout=PHOTO_DOWNLOAD_TIMEOUT_SECONDS) as response:
            content = response.read()
            if not content:
                return None
            content_type = response_content_type(response)
    except (HTTPError, URLError, OSError) as exc:
        print(
            f"WARNING: Failed to download photo for {slug}:{place_id} from {photo_url}: {exc}",
            flush=True,
        )
        return None

    optimized_content, extension = optimize_place_photo_asset(
        content,
        content_type=content_type,
    )
    if optimized_content is None:
        return None

    place_prefix = canonical_place_photo_stem(place_id, photo_url)
    filename = canonical_place_photo_filename(place_id, photo_url, extension=extension)
    output_path = photo_dir / filename
    temp_path = photo_dir / f".{filename}.tmp"
    temp_path.write_bytes(optimized_content)
    temp_path.replace(output_path)

    for stale_path in photo_dir.glob(f"{place_prefix}-*"):
        if stale_path.name == filename or stale_path.name.startswith("."):
            continue
        stale_path.unlink(missing_ok=True)

    remove_legacy_place_photo_matches(slug, filename_glob=canonical_place_photo_glob(place_id, photo_url))
    return public_photo_path(filename)


def safe_place_photo_stem(place_id: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "-", place_id).strip("-._")
    return sanitized or "place"


def canonical_place_photo_glob(place_id: str, photo_url: str) -> str:
    return f"{canonical_place_photo_stem(place_id, photo_url)}.*"


def canonical_place_photo_filename(place_id: str, photo_url: str, *, extension: str) -> str:
    return f"{canonical_place_photo_stem(place_id, photo_url)}{extension}"


def canonical_place_photo_stem(place_id: str, photo_url: str) -> str:
    place_prefix = safe_place_photo_stem(place_id)
    photo_hash = hashlib.sha256(photo_url.encode("utf-8")).hexdigest()[:12]
    return f"{place_prefix}-{photo_hash}"


def migrate_legacy_place_photo_to_flat_dir(
    slug: str,
    *,
    filename_glob: str,
    legacy_path: Path,
) -> Path:
    canonical_path = PLACE_PHOTOS_DIR / legacy_path.name
    canonical_path.parent.mkdir(parents=True, exist_ok=True)
    if canonical_path.exists():
        legacy_path.unlink(missing_ok=True)
    else:
        legacy_path.replace(canonical_path)

    remove_legacy_place_photo_matches(slug, filename_glob=filename_glob)
    return canonical_path


def remove_legacy_place_photo_matches(slug: str, *, filename_glob: str) -> None:
    legacy_dir = PLACE_PHOTOS_DIR / slug
    if not legacy_dir.exists():
        return

    for stale_path in legacy_dir.glob(filename_glob):
        stale_path.unlink(missing_ok=True)

    try:
        next(legacy_dir.iterdir())
    except StopIteration:
        legacy_dir.rmdir()
    except FileNotFoundError:
        return


def response_content_type(response: Any) -> str | None:
    headers = getattr(response, "headers", None)
    if headers is None:
        return None
    get_content_type = getattr(headers, "get_content_type", None)
    if callable(get_content_type):
        return get_content_type()
    get = getattr(headers, "get", None)
    if callable(get):
        return as_string(get("Content-Type"))
    return None


def optimize_place_photo_asset(
    content: bytes,
    *,
    content_type: str | None,
) -> tuple[bytes, str] | tuple[None, None]:
    try:
        with Image.open(io.BytesIO(content)) as image:
            image.load()
            working = ImageOps.exif_transpose(image)
            resized = ImageOps.fit(
                working,
                (PHOTO_CARD_WIDTH, PHOTO_CARD_HEIGHT),
                method=Image.Resampling.LANCZOS,
                centering=(0.5, 0.5),
            )

            output = io.BytesIO()
            if image_supports_webp():
                if resized.mode not in {"RGB", "RGBA"}:
                    resized = resized.convert("RGB")
                resized.save(
                    output,
                    format="WEBP",
                    quality=PHOTO_CARD_QUALITY,
                    method=6,
                )
                return output.getvalue(), ".webp"

            if resized.mode != "RGB":
                resized = resized.convert("RGB")
            resized.save(
                output,
                format="JPEG",
                quality=PHOTO_CARD_QUALITY,
                optimize=True,
            )
            return output.getvalue(), ".jpg"
    except (OSError, UnidentifiedImageError) as exc:
        print(
            f"WARNING: Failed to optimize downloaded photo ({content_type or 'unknown'}): {exc}",
            flush=True,
        )
        return None, None


def image_supports_webp() -> bool:
    return bool(features.check("webp"))


def public_photo_path(filename: str) -> str:
    return f"/place-photos/{filename}"


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
    place_url = build_public_google_maps_url(
        name=place.name,
        address=place.address,
        lat=place.lat,
        lng=place.lng,
        raw_maps_url=place.maps_url,
    )
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
        last_error: str | None = None
        saw_non_error_result = False
        for scrape_url in build_place_page_candidate_urls(place):
            try:
                details = scrape_place(
                    scrape_url,
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
                            scrape_url,
                            headless=True,
                            browser_session=browser_session,
                            http_session=http_session,
                        )
                    except RECOVERABLE_REFRESH_ERRORS as retry_exc:
                        last_error = f"scrape_error:{retry_exc}"
                        continue
                else:
                    last_error = f"scrape_error:{exc}"
                    continue

            saw_non_error_result = True
            record_scraper_session_use(session_state, proxy=proxy)
            enrichment_place = normalize_place_page_enrichment(details)
            source_url = as_string(getattr(details, "source_url", None)) or enrichment_place.google_maps_uri
            if source_url and "/maps/search/" in source_url and not enrichment_place.formatted_address:
                continue
            matched = place_page_has_meaningful_enrichment(details, enrichment_place)
            if matched and not place_page_candidate_is_confident_match(place, details, enrichment_place):
                continue
            if matched:
                return build_cache_entry(
                    place,
                    source="google_maps_page",
                    query=query,
                    matched=True,
                    score=STRONG_MATCH_SCORE,
                    enrichment_place=enrichment_place,
                )
        if saw_non_error_result:
            return build_cache_entry(
                place,
                source="google_maps_page",
                query=query,
                matched=False,
            )
        if last_error is not None:
            return build_cache_entry(
                place,
                source="google_maps_page",
                query=query,
                error=last_error,
            )
        return build_cache_entry(
            place,
            source="google_maps_page",
            query=query,
            matched=False,
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
    is_search_result = bool(place.google_maps_uri and "/maps/search/" in place.google_maps_uri)
    has_reputation = place.rating is not None or place.user_rating_count is not None
    has_contact = bool(place.website or place.phone)
    if is_search_result and not has_reputation and not has_contact:
        return True
    if place.business_status is None and place.rating is None and place.user_rating_count is None:
        if not place.primary_type_display_name:
            return True
    return False


def place_page_has_meaningful_enrichment(
    details: Any,
    enrichment_place: EnrichmentPlace,
) -> bool:
    has_identity = bool(enrichment_place.display_name and enrichment_place.formatted_address)
    has_reputation = (
        enrichment_place.rating is not None
        or enrichment_place.user_rating_count is not None
    )
    has_contact_or_context = bool(
        enrichment_place.primary_type_display_name
        or enrichment_place.website
        or enrichment_place.phone
        or enrichment_place.plus_code
        or enrichment_place.description
    )
    if has_identity and (has_reputation or has_contact_or_context):
        return True
    if (
        enrichment_place.display_name
        and has_contact_or_context
        and not bool(getattr(details, "limited_view", False))
    ):
        return True
    return not bool(getattr(details, "limited_view", False)) and bool(
        has_reputation and enrichment_place.formatted_address
    )


def place_page_candidate_is_confident_match(
    raw_place: RawPlace,
    details: Any,
    enrichment_place: EnrichmentPlace,
) -> bool:
    source_url = as_string(getattr(details, "source_url", None)) or enrichment_place.google_maps_uri
    if not source_url or "/maps/search/" not in source_url:
        return True
    return score_place_page_candidate(raw_place, details, enrichment_place) >= 25


def score_place_page_candidate(
    raw_place: RawPlace,
    details: Any,
    enrichment_place: EnrichmentPlace,
) -> int:
    score = 0
    raw_name = normalize_text(raw_place.name)
    candidate_name = normalize_text(
        enrichment_place.display_name or as_string(getattr(details, "name", None))
    )
    raw_address = normalize_text(raw_place.address)
    candidate_address = normalize_text(
        enrichment_place.formatted_address or as_string(getattr(details, "address", None))
    )

    if raw_name and candidate_name:
        if raw_name == candidate_name:
            score += 50
        elif raw_name in candidate_name or candidate_name in raw_name:
            score += 30
        else:
            score += token_overlap_score(raw_name, candidate_name)

    if raw_address and candidate_address:
        score += token_overlap_score(raw_address, candidate_address)

    raw_lat = raw_place.lat
    raw_lng = raw_place.lng
    candidate_lat = as_float(getattr(details, "lat", None))
    candidate_lng = as_float(getattr(details, "lng", None))
    if raw_lat is not None and raw_lng is not None and candidate_lat is not None and candidate_lng is not None:
        distance_m = haversine_meters(raw_lat, raw_lng, candidate_lat, candidate_lng)
        if distance_m <= 100:
            score += 40
        elif distance_m <= 400:
            score += 25
        elif distance_m <= 1200:
            score += 10

    return score


def build_place_page_candidate_urls(place: RawPlace) -> list[str]:
    maps_url = as_string(place.maps_url)
    if maps_url is None:
        return []

    query = build_text_query(place)
    search_url = localize_google_maps_scrape_url(build_google_maps_search_url(query)) if query else None
    cid = as_string(place.cid) or extract_maps_cid(maps_url)
    cid_url = localize_google_maps_scrape_url(f"https://maps.google.com/?cid={cid}") if cid else None

    candidates: list[str] = []
    if should_prefer_search_place_url(maps_url) and search_url is not None:
        candidates.append(search_url)
    candidates.append(localize_google_maps_scrape_url(maps_url))
    if cid_url is not None:
        candidates.append(cid_url)
    if not should_prefer_search_place_url(maps_url) and search_url is not None:
        candidates.append(search_url)
    return dedupe_urls(candidates)


def localize_google_maps_scrape_url(url: str) -> str:
    split = urlsplit(url)
    host = split.netloc.lower()
    if "google." not in host:
        return url

    query_pairs = [(key, value) for key, value in parse_qsl(split.query, keep_blank_values=True) if key not in {"hl", "gl"}]
    query_pairs.extend([("hl", "en"), ("gl", "us")])
    return urlunsplit(split._replace(query=urlencode(query_pairs)))


def should_prefer_search_place_url(maps_url: str) -> bool:
    normalized = maps_url.strip().lower()
    if "/maps/place/" in normalized or "/maps/search/" in normalized:
        return False
    return "?cid=" in normalized or "?q=" in normalized or "&cid=" in normalized or "&q=" in normalized


def dedupe_urls(urls: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for url in urls:
        if url in seen:
            continue
        deduped.append(url)
        seen.add(url)
    return deduped


PLACE_PAGE_ADDRESS_REJECT_SUBSTRINGS = (
    "about this data",
    "faviconv2",
    "imagery ©",
    "map data ©",
    "send product feedback",
    "street view",
    "termsprivacy",
)
PLACE_PAGE_ADDRESS_REJECT_HOST_FRAGMENTS = ("gstatic.com", "googleusercontent.com")
PLACE_PAGE_ADDRESS_ENTITY_TOKEN_RE = re.compile(r"^/(?:g|m)/[A-Za-z0-9_-]+$")


def sanitize_place_page_formatted_address(value: Any) -> str | None:
    normalized = as_string(value)
    if normalized is None:
        return None

    lowered = normalized.lower()
    if lowered.startswith(("http://", "https://", "www.")):
        return None
    if any(fragment in lowered for fragment in PLACE_PAGE_ADDRESS_REJECT_SUBSTRINGS):
        return None
    if any(fragment in lowered for fragment in PLACE_PAGE_ADDRESS_REJECT_HOST_FRAGMENTS):
        return None
    if PLACE_PAGE_ADDRESS_ENTITY_TOKEN_RE.fullmatch(normalized):
        return None
    if re.search(r"\d", normalized) and re.search(r"\breviews?\b", lowered):
        return None
    if normalized.endswith(".") and re.search(r"\b[23456789CFGHJMPQRVWX]{4,8}\+[23456789CFGHJMPQRVWX]{2,3}\b", normalized) is None:
        return None

    if "·" in normalized:
        segments = [segment.strip() for segment in normalized.split("·") if segment.strip()]
        for candidate in reversed(segments):
            cleaned = sanitize_place_page_formatted_address(candidate)
            if cleaned is not None and cleaned != normalized:
                return cleaned
        return None

    return normalized


def normalize_place_page_enrichment(details: Any) -> EnrichmentPlace:
    raw_category = sanitize_enrichment_primary_category(as_string(getattr(details, "category", None)))
    primary_type = None
    types: list[str] = []
    if raw_category is not None:
        primary_type, types = normalized_enrichment_type_ids(
            slugify(raw_category).replace("-", "_"),
            raw_category,
            [slugify(raw_category).replace("-", "_")],
        )
    category = canonical_primary_category_label(primary_type=primary_type, display_name=raw_category)
    category_localized = localized_primary_category_label(
        raw_display_name=raw_category,
        canonical_display_name=category,
    )

    source_url = as_string(getattr(details, "source_url", None))
    resolved_url = as_string(getattr(details, "resolved_url", None))
    from_search_url = bool(source_url and "/maps/search/" in source_url)
    display_name = as_string(getattr(details, "name", None))
    formatted_address = sanitize_place_page_formatted_address(getattr(details, "address", None))
    rating = as_float(getattr(details, "rating", None))
    user_rating_count = as_int(getattr(details, "review_count", None))
    website = as_string(getattr(details, "website", None))
    phone = as_string(getattr(details, "phone", None))
    plus_code = as_string(getattr(details, "plus_code", None))
    if formatted_address is None and plus_code and any(
        separator in plus_code for separator in (" - ", ",", " ")
    ):
        formatted_address = sanitize_place_page_formatted_address(plus_code)
    description = as_string(getattr(details, "description", None))
    if formatted_address is None:
        description_address = sanitize_place_page_formatted_address(description)
        if description_address is not None:
            formatted_address = description_address
            description = None
    main_photo_url = as_string(getattr(details, "main_photo_url", None))
    photo_url = as_string(getattr(details, "photo_url", None))
    if from_search_url:
        description = None
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
            main_photo_url,
            photo_url,
        )
    )
    maps_uri = None
    if limited_view or has_meaningful_fields:
        maps_uri = source_url if from_search_url else resolved_url or source_url
    return EnrichmentPlace(
        display_name=display_name,
        formatted_address=formatted_address,
        google_maps_uri=maps_uri,
        rating=rating,
        user_rating_count=user_rating_count,
        primary_type=primary_type,
        primary_type_display_name=category,
        primary_type_display_name_localized=category_localized,
        types=types,
        business_status=normalize_place_page_business_status(as_string(getattr(details, "status", None))),
        website=website,
        phone=phone,
        plus_code=plus_code,
        description=description,
        main_photo_url=main_photo_url,
        photo_url=photo_url,
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


def build_maps_link_query(
    *,
    name: str | None,
    address: str | None,
    lat: float | None,
    lng: float | None,
) -> str | None:
    text_parts = [part.strip() for part in (name, address) if isinstance(part, str) and part.strip()]
    if text_parts:
        return ", ".join(dict.fromkeys(text_parts))
    if lat is not None and lng is not None:
        return f"{lat:.7f},{lng:.7f}"
    return None


def build_google_maps_search_url(query: str, *, google_place_id: str | None = None) -> str:
    params = {"api": "1", "query": query}
    if google_place_id:
        params["query_place_id"] = google_place_id
    return f"https://www.google.com/maps/search/?{urlencode(params)}"


def should_rebuild_google_maps_url(maps_url: str | None) -> bool:
    normalized_url = as_string(maps_url)
    if normalized_url is None:
        return True
    if re.search(r"[?&]cid=[^&#]+", normalized_url):
        return True
    if "google." not in normalized_url:
        return False
    return bool(re.search(r"[?&]q=-?\d+(?:\.\d+)?,-?\d+(?:\.\d+)?(?:[&#]|$)", normalized_url))


def build_public_google_maps_url(
    *,
    name: str | None,
    address: str | None,
    lat: float | None,
    lng: float | None,
    raw_maps_url: str | None,
    google_maps_uri: str | None = None,
    google_place_id: str | None = None,
) -> str:
    query = build_maps_link_query(name=name, address=address, lat=lat, lng=lng)
    if query and google_place_id:
        return build_google_maps_search_url(query, google_place_id=google_place_id)

    preferred_url = as_string(google_maps_uri) or as_string(raw_maps_url)
    if preferred_url and not should_rebuild_google_maps_url(preferred_url):
        return preferred_url

    if query:
        return build_google_maps_search_url(query)

    return preferred_url or raw_maps_url or "https://www.google.com/maps"


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
    raw_primary_type_display_name = sanitize_enrichment_primary_category(
        display_name_text(candidate.get("primaryTypeDisplayName"))
    )
    primary_type = as_string(candidate.get("primaryType"))
    primary_type_display_name = canonical_primary_category_label(
        primary_type=primary_type,
        display_name=raw_primary_type_display_name,
    )
    primary_type_display_name_localized = localized_primary_category_label(
        raw_display_name=raw_primary_type_display_name,
        canonical_display_name=primary_type_display_name,
    )
    primary_type, types = normalized_enrichment_type_ids(
        primary_type,
        primary_type_display_name,
        coerce_string_list(candidate.get("types")),
    )
    return EnrichmentPlace(
        google_place_id=as_string(candidate.get("id")),
        google_place_resource_name=as_string(candidate.get("name")),
        display_name=display_name_text(display_name),
        formatted_address=as_string(candidate.get("formattedAddress")),
        google_maps_uri=as_string(candidate.get("googleMapsUri")),
        rating=as_float(candidate.get("rating")),
        user_rating_count=as_int(candidate.get("userRatingCount")),
        primary_type=primary_type,
        primary_type_display_name=primary_type_display_name,
        primary_type_display_name_localized=primary_type_display_name_localized,
        types=types,
        business_status=as_string(candidate.get("businessStatus")),
    )


def coerce_enrichment_place(value: EnrichmentCacheEntry | None) -> EnrichmentPlace:
    if value is None or value.place is None:
        return EnrichmentPlace()
    return canonicalize_enrichment_place(value.place) or EnrichmentPlace()


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


def format_duration_seconds(duration_seconds: float) -> str:
    total_tenths = max(0, int(max(0.0, duration_seconds) * 10 + 0.5))
    minutes, remaining_tenths = divmod(total_tenths, 600)
    seconds, tenths = divmod(remaining_tenths, 10)
    if minutes >= 1:
        return f"{minutes}m {seconds:02d}.{tenths}s"
    return f"{seconds}.{tenths}s"


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

    if args.enrich or args.enrich_missing or args.refresh_enrichment:
        sync_local_csv_sources()
        enrich_raw_sources(
            force_refresh=args.refresh_enrichment,
            missing_only=args.enrich_missing,
            refresh_workers=args.refresh_workers,
            refresh_startup_jitter_seconds=args.refresh_startup_jitter_seconds,
        )

    if args.export_cache_json:
        exported_guides = export_all_places_cache_json()
        print(f"Exported JSON cache debug files for {exported_guides} guide(s).")

    build_started_at = time.perf_counter()
    rebuild_generated_data(
        refresh_photos=args.refresh_photos,
        photo_workers=args.refresh_workers,
        startup_jitter_seconds=args.refresh_startup_jitter_seconds,
    )
    build_duration = time.perf_counter() - build_started_at
    print(f"Generated list data, manifests, and search index in {format_duration_seconds(build_duration)}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
