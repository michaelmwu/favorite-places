from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from pydantic import TypeAdapter

ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "scripts" / "config" / "list_sources.json"
RAW_DIR = ROOT / "data" / "raw"
PLACES_CACHE_DIR = ROOT / "data" / "cache" / "google-places"
GENERATED_DIR = ROOT / "src" / "data" / "generated"
GENERATED_LISTS_DIR = GENERATED_DIR / "lists"
LIST_OVERRIDES_DIR = ROOT / "src" / "data" / "overrides" / "lists"
PLACE_OVERRIDES_DIR = ROOT / "src" / "data" / "overrides" / "places"
DEFAULT_RAW_REFRESH_TTL = timedelta(days=14)
DEFAULT_REFRESH_WORKERS = 4
ERROR_CACHE_TTL = timedelta(days=1)
UNMATCHED_CACHE_TTL = timedelta(days=3)
LOW_CONFIDENCE_CACHE_TTL = timedelta(days=3)
RATINGS_CACHE_TTL = timedelta(days=7)
NON_OPERATIONAL_CACHE_TTL = timedelta(days=3)
OPERATIONAL_CACHE_TTL = timedelta(days=14)
STRONG_MATCH_SCORE = 45
PLACES_TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
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
)

try:
    from google_saved_lists import scrape_saved_list
except ImportError:
    scrape_saved_list = None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-scrape configured public Google Maps lists before rebuilding generated JSON.",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Run the scraper in headed mode.",
    )
    parser.add_argument(
        "--refresh-force",
        action="store_true",
        help="Force-refresh raw list scrapes even if the stored list is still within its refresh window.",
    )
    parser.add_argument(
        "--refresh-list",
        action="append",
        default=[],
        help="Force-refresh only the configured list matching this slug or source URL. Repeat to target multiple lists.",
    )
    parser.add_argument(
        "--refresh-workers",
        type=int,
        default=DEFAULT_REFRESH_WORKERS,
        help="Maximum parallel workers for headless raw list refreshes. Headed refreshes stay single-worker.",
    )
    parser.add_argument(
        "--enrich",
        action="store_true",
        help="Fill missing or stale Google Places enrichment cache entries.",
    )
    parser.add_argument(
        "--refresh-enrichment",
        action="store_true",
        help="Force-refresh Google Places enrichment cache entries for every place.",
    )
    return parser


def load_sources() -> list[SourceConfig]:
    if not CONFIG_PATH.exists():
        return []
    return TypeAdapter(list[SourceConfig]).validate_json(CONFIG_PATH.read_text(encoding="utf-8"))


def refresh_raw_sources(
    *,
    headed: bool,
    force_refresh: bool,
    refresh_lists: list[str],
    refresh_workers: int,
) -> None:
    if scrape_saved_list is None:
        raise RuntimeError(
            "Could not import google_saved_lists. "
            "Run `uv sync` to install the scraper dependency before using --refresh."
        )

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    sources = load_sources()
    selected_sources = resolve_refresh_sources(sources, refresh_lists)
    selected_keys = {(source.slug, source.url) for source in selected_sources}
    refresh_jobs: list[tuple[SourceConfig, Path]] = []

    for source in sources:
        if selected_sources and (source.slug, source.url) not in selected_keys:
            continue

        raw_path = RAW_DIR / f"{source.slug}.json"
        existing_payload = load_raw_saved_list(raw_path)
        should_force_refresh = force_refresh or bool(selected_sources)
        skip_reason = None if should_force_refresh else raw_refresh_skip_reason(existing_payload, source)
        if skip_reason is not None:
            print(f"Skipping {source.slug} ({skip_reason})")
            continue

        print(f"Refreshing {source.slug} from {source.url}")
        refresh_jobs.append((source, raw_path))

    if not refresh_jobs:
        return

    max_workers = max(1, refresh_workers)
    if headed or len(refresh_jobs) == 1 or max_workers == 1:
        for source, raw_path in refresh_jobs:
            write_json(raw_path, scrape_raw_source(source, headed=headed))
        return

    max_workers = min(max_workers, len(refresh_jobs))
    print(f"Running {len(refresh_jobs)} headless refresh jobs with {max_workers} workers")
    payloads_by_slug: dict[str, RawSavedList] = {}
    failures: list[str] = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(scrape_raw_source, source, headed=False): (source, raw_path)
            for source, raw_path in refresh_jobs
        }
        for future in as_completed(future_map):
            source, _raw_path = future_map[future]
            try:
                payloads_by_slug[source.slug] = future.result()
            except Exception as exc:
                failures.append(f"{source.slug}: {exc}")

    for source, raw_path in refresh_jobs:
        payload = payloads_by_slug.get(source.slug)
        if payload is not None:
            write_json(raw_path, payload)

    if failures:
        failure_text = "\n".join(failures)
        raise RuntimeError(f"Raw refresh failed for {len(failures)} source(s):\n{failure_text}")


def scrape_raw_source(source: SourceConfig, *, headed: bool) -> RawSavedList:
    if scrape_saved_list is None:
        raise RuntimeError(
            "Could not import google_saved_lists. "
            "Run `uv sync` to install the scraper dependency before using --refresh."
        )

    result = scrape_saved_list(source.url, headless=not headed)
    payload = RawSavedList.model_validate(result.to_dict())
    if source.title and not payload.title:
        payload.title = source.title
    stamp_raw_saved_list(payload, source)
    return payload


def rebuild_generated_data() -> None:
    GENERATED_LISTS_DIR.mkdir(parents=True, exist_ok=True)
    guides: list[Guide] = []
    search_index: list[dict[str, Any]] = []

    for raw_path in sorted(RAW_DIR.glob("*.json")):
        raw = RawSavedList.model_validate_json(raw_path.read_text(encoding="utf-8"))
        enrichment_cache = load_places_cache(raw_path.stem)
        guide = normalize_guide(raw_path.stem, raw, enrichment_cache=enrichment_cache)
        guides.append(guide)
        write_json(GENERATED_LISTS_DIR / f"{guide.slug}.json", guide)

        for place in guide.places:
            if place.hidden:
                continue
            search_index.append(
                {
                    "id": place.id,
                    "list_slug": guide.slug,
                    "list_title": guide.title,
                    "name": place.name,
                    "tags": place.tags,
                    "category": place.primary_category,
                    "neighborhood": place.neighborhood,
                    "search_text": " ".join(
                        filter(
                            None,
                            [
                                place.name,
                                place.address,
                                place.note,
                                place.why_recommended,
                                place.primary_category,
                                place.neighborhood,
                                " ".join(place.tags),
                            ],
                        )
                    ).lower(),
                }
            )

    guides.sort(key=lambda guide: (guide.country_name, guide.city_name, guide.title))
    manifests = [summarize_guide(guide) for guide in guides]
    write_json(GENERATED_DIR / "manifests.json", manifests)
    write_json(GENERATED_DIR / "search-index.json", search_index)


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

    for place in raw.places:
        place_id = stable_place_id(place)
        override = place_override_map.get(place_id, {})
        enrichment = coerce_enrichment_place(enrichment_cache.get(place_id))
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
        neighborhood = as_string(override.get("neighborhood")) or infer_neighborhood(place.address)
        hidden = bool(override.get("hidden", False))
        top_pick_override = as_bool(override.get("top_pick"))
        top_pick = top_pick_override if top_pick_override is not None else place.is_favorite
        note = as_string(override.get("note")) or place.note
        why_recommended = as_string(override.get("why_recommended"))
        manual_rank = as_int(override.get("manual_rank")) or 0
        status = (
            as_string(override.get("status"))
            or normalize_business_status(enrichment.business_status)
            or "active"
        )

        normalized = NormalizedPlace(
            id=place_id,
            name=place.name,
            address=place.address or enrichment.formatted_address,
            lat=place.lat,
            lng=place.lng,
            maps_url=enrichment.google_maps_uri or place.maps_url,
            cid=place.cid,
            google_id=place.google_id,
            google_place_id=enrichment.google_place_id,
            google_place_resource_name=enrichment.google_place_resource_name,
            primary_category=primary_category,
            tags=tags,
            neighborhood=neighborhood,
            note=note,
            why_recommended=why_recommended,
            top_pick=top_pick,
            hidden=hidden,
            manual_rank=manual_rank,
            status=status,
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
        places=normalized_places,
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
        city_name=guide.city_name,
        list_tags=guide.list_tags,
        place_count=guide.place_count,
        featured_names=featured_names[:3],
        top_categories=guide.top_categories,
    )


def enrich_raw_sources(*, force_refresh: bool) -> None:
    api_key = google_places_api_key()
    if api_key is None:
        raise RuntimeError(
            "Google Places enrichment needs GOOGLE_PLACES_API_KEY or GOOGLE_MAPS_API_KEY in the environment."
        )

    for raw_path in sorted(RAW_DIR.glob("*.json")):
        raw = RawSavedList.model_validate_json(raw_path.read_text(encoding="utf-8"))
        cache_payload = load_places_cache(raw_path.stem)
        changed = False
        for place in raw.places:
            place_id = stable_place_id(place)
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


def stable_place_id(place: RawPlace) -> str:
    cid = place.cid
    if cid:
        return f"cid:{cid}"

    google_id = place.google_id
    if google_id:
        normalized = google_id.strip("/").replace("/", "-")
        return f"gid:{normalized}"

    fallback = f"{place.name or 'place'}-{place.lat}-{place.lng}"
    return f"slug:{slugify(fallback)}"


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
    address = place.address
    neighborhood = infer_neighborhood(address)
    if neighborhood:
        tags.add(slugify(neighborhood))
    if category:
        tags.add(slugify(category))
    for place_type in enrichment.types[:4]:
        tags.add(slugify(place_type.replace("_", "-")))
    return sorted(tag for tag in tags if tag)


def infer_city_name(title: str) -> str | None:
    parts = split_title_parts(title)
    if not parts:
        return None
    return parts[0]


def infer_country_name(title: str, raw: RawSavedList) -> str | None:
    parts = split_title_parts(title)
    if len(parts) > 1:
        return parts[-1]

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
    return mapping.get(country_name)


def infer_country_from_address(address: str | None) -> str | None:
    if address is None:
        return None
    parts = [part.strip() for part in address.split(",") if part.strip()]
    if not parts:
        return None
    tail = re.sub(r"[^\w\s-]", "", parts[-1]).strip()
    return tail or None


def infer_neighborhood(address: str | None) -> str | None:
    if address is None:
        return None
    parts = [part.strip() for part in address.split(",") if part.strip()]
    if len(parts) >= 2:
        candidate = parts[-2]
        cleaned = re.sub(r"^[0-9]+\s*", "", candidate).strip()
        return cleaned or None
    return None


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


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if hasattr(payload, "model_dump"):
        payload = payload.model_dump(mode="json")
    elif isinstance(payload, list):
        payload = [item.model_dump(mode="json") if hasattr(item, "model_dump") else item for item in payload]
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


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


def raw_source_signature(source: SourceConfig) -> str:
    payload = {
        "slug": source.slug,
        "url": source.url,
        "title": source.title,
        "refresh_days": source.refresh_days,
    }
    serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def raw_refresh_ttl(source: SourceConfig) -> timedelta:
    refresh_days = source.refresh_days
    if isinstance(refresh_days, int) and refresh_days > 0:
        return timedelta(days=refresh_days)
    return DEFAULT_RAW_REFRESH_TTL


def stamp_raw_saved_list(payload: RawSavedList, source: SourceConfig) -> None:
    fetched_at = datetime.now(UTC)
    payload.fetched_at = fetched_at.isoformat()
    payload.refresh_after = (fetched_at + raw_refresh_ttl(source)).isoformat()
    payload.source_signature = raw_source_signature(source)
    payload.configured_source_url = source.url


def raw_refresh_skip_reason(payload: RawSavedList | None, source: SourceConfig) -> str | None:
    if payload is None:
        return None
    if payload.source_signature != raw_source_signature(source):
        return None
    if payload.refresh_after:
        try:
            refresh_after_dt = datetime.fromisoformat(payload.refresh_after)
        except ValueError:
            return None
        if datetime.now(UTC) < refresh_after_dt:
            return f"fresh until {payload.refresh_after}"
        return None
    if payload.fetched_at:
        try:
            fetched_at_dt = datetime.fromisoformat(payload.fetched_at)
        except ValueError:
            return None
        if datetime.now(UTC) - fetched_at_dt < raw_refresh_ttl(source):
            return f"fresh from {payload.fetched_at}"
    return None


def resolve_refresh_sources(sources: list[SourceConfig], selectors: list[str]) -> list[SourceConfig]:
    if not selectors:
        return []

    normalized_selectors = {selector.strip() for selector in selectors if selector.strip()}
    matched_sources: list[SourceConfig] = []
    missing_selectors = set(normalized_selectors)

    for source in sources:
        if source.slug in normalized_selectors or source.url in normalized_selectors:
            matched_sources.append(source)
            missing_selectors.discard(source.slug)
            missing_selectors.discard(source.url)

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
        "cid": place.cid,
        "google_id": place.google_id,
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


def fetch_places_enrichment(place: RawPlace, *, api_key: str) -> EnrichmentCacheEntry:
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
            query=query,
            error=f"http_{exc.code}",
            error_body=body[:500],
        )
    except URLError as exc:
        return build_cache_entry(
            place,
            query=query,
            error=f"url_error:{exc.reason}",
        )

    matches = payload.get("places")
    if not isinstance(matches, list) or not matches:
        return build_cache_entry(
            place,
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

    if args.refresh_list and not args.refresh:
        args.refresh = True

    if args.refresh:
        refresh_raw_sources(
            headed=args.headed,
            force_refresh=args.refresh_force,
            refresh_lists=args.refresh_list,
            refresh_workers=args.refresh_workers,
        )

    if args.enrich or args.refresh_enrichment:
        enrich_raw_sources(force_refresh=args.refresh_enrichment)

    rebuild_generated_data()
    print("Generated list data, manifests, and search index.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
