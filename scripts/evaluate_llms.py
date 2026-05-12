"""Evaluate LLM candidates for semantic enrichment and Google Maps DOM repair."""

from __future__ import annotations

import argparse
import json
import os
import random
import sqlite3
import sys
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

try:
    from scripts import build_data
    from scripts.pipeline_models import EnrichmentCacheEntry, RawPlace, RawSavedList
except ModuleNotFoundError:
    import build_data  # type: ignore[no-redef]
    from pipeline_models import EnrichmentCacheEntry, RawPlace, RawSavedList

try:
    from gmaps_scraper import PlaceLLMRepairRequest, scrape_place
    from gmaps_scraper.models import PLACE_LLM_DISPLAY_TRANSLATION_FIELDS, PLACE_LLM_DOM_REPAIR_FIELDS
except ImportError:
    PlaceLLMRepairRequest = Any  # type: ignore[misc,assignment]
    scrape_place = None  # type: ignore[assignment]
    PLACE_LLM_DISPLAY_TRANSLATION_FIELDS = ()
    PLACE_LLM_DOM_REPAIR_FIELDS = ()


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_ROOT = ROOT / ".context" / "llm-evals"
DEFAULT_TIMEOUT_SECONDS = 90.0
DEFAULT_SEMANTIC_LIMIT = 12
DEFAULT_DOM_LIMIT = 6


@dataclass(frozen=True)
class ModelProfile:
    name: str
    provider: Literal["openai-compatible", "anthropic"]
    model: str
    api_key_env: str
    base_url: str | None = None
    request_options: dict[str, Any] = field(default_factory=dict)
    cost_per_1m: dict[str, float] = field(default_factory=dict)

    def api_key(self) -> str:
        value = os.environ.get(self.api_key_env) or load_dotenv_values(ROOT / ".env").get(self.api_key_env)
        if not value:
            raise RuntimeError(f"{self.api_key_env} is required for model profile {self.name}.")
        return value


DEFAULT_MODEL_PROFILES_PATH = ROOT / "scripts" / "llm_model_profiles.json"


def add_global_arguments(parser: argparse.ArgumentParser, *, suppress_defaults: bool = False) -> None:
    default = argparse.SUPPRESS if suppress_defaults else None
    parser.add_argument(
        "--models",
        default=argparse.SUPPRESS if suppress_defaults else "kimi-k2p6-turbo-fireworks,gpt-5.5",
        help="Comma-separated model profile names or model ids. Unknown ids use OpenAI defaults unless --base-url is set.",
    )
    parser.add_argument(
        "--baseline",
        default=argparse.SUPPRESS if suppress_defaults else "gpt-5.5",
        help="Model name to label as baseline in judge packs.",
    )
    parser.add_argument(
        "--profiles",
        type=Path,
        default=default,
        help=(
            "Optional JSON file with extra/overridden model profiles layered on top of "
            "scripts/llm_model_profiles.json."
        ),
    )
    parser.add_argument("--output-root", type=Path, default=argparse.SUPPRESS if suppress_defaults else DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--run-name", default=default, help="Output directory name. Defaults to a UTC timestamp.")
    parser.add_argument("--seed", type=int, default=argparse.SUPPRESS if suppress_defaults else 7)
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=argparse.SUPPRESS if suppress_defaults else DEFAULT_TIMEOUT_SECONDS,
    )
    parser.add_argument("--base-url", default=default, help="Base URL for unknown OpenAI-compatible model ids.")
    parser.add_argument(
        "--api-key-env",
        default=default,
        help=(
            "Environment variable for unknown model ids. Defaults to OPENAI_API_KEY, or LLM_API_KEY when "
            "--base-url is set."
        ),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    add_global_arguments(parser)
    subparsers = parser.add_subparsers(dest="command", required=True)

    semantic = subparsers.add_parser("semantic", help="Evaluate semantic description, tags, vibe tags, and type tags.")
    add_global_arguments(semantic, suppress_defaults=True)
    semantic.add_argument("--limit", type=int, default=DEFAULT_SEMANTIC_LIMIT)
    semantic.add_argument("--guide", action="append", dest="guides", help="Guide slug to include. Repeatable.")
    semantic.add_argument("--place", action="append", dest="places", help="Place selector substring/id to include. Repeatable.")
    semantic.add_argument("--include-noted", action="store_true", help="Include raw places with handwritten notes.")
    semantic.add_argument("--cases-file", type=Path, help="Replay previously captured semantic cases from JSONL.")
    semantic.add_argument("--capture-only", action="store_true", help="Capture semantic cases and skip model calls.")
    semantic.add_argument(
        "--fixture-suite",
        choices=("random", "stratified"),
        default="stratified",
        help="How to select semantic cases when --cases-file is not provided.",
    )
    semantic.add_argument("--force", action="store_true", help="Ignore cached eval model responses.")

    dom = subparsers.add_parser("dom-repair", help="Evaluate DOM repair JSON from live Google Maps place evidence.")
    add_global_arguments(dom, suppress_defaults=True)
    dom.add_argument("--limit", type=int, default=DEFAULT_DOM_LIMIT)
    dom.add_argument("--guide", action="append", dest="guides", help="Guide slug to include. Repeatable.")
    dom.add_argument("--place", action="append", dest="places", help="Place selector substring/id to include. Repeatable.")
    dom.add_argument("--headful", action="store_true", help="Run the scraper browser visibly.")
    dom.add_argument("--collect-reviews", action="store_true", help="Collect reviews during repair case capture.")
    dom.add_argument("--collect-about", action="store_true", help="Collect about sections during repair case capture.")
    dom.add_argument("--cases-file", type=Path, help="Replay previously captured DOM repair cases from JSONL.")
    dom.add_argument("--capture-only", action="store_true", help="Capture DOM repair cases and skip model calls.")
    dom.add_argument("--force", action="store_true", help="Ignore cached eval model responses.")

    args = parser.parse_args(argv)
    random.seed(args.seed)
    profiles = resolve_model_profiles(
        args.models,
        profiles_path=args.profiles,
        fallback_base_url=args.base_url,
        fallback_api_key_env=args.api_key_env,
    )
    run_dir = build_run_dir(args.output_root, args.run_name, args.command)
    run_dir.mkdir(parents=True, exist_ok=True)
    write_json(
        run_dir / "run.json",
        {
            "created_at": now_iso(),
            "command": args.command,
            "default_profiles_path": str(DEFAULT_MODEL_PROFILES_PATH.relative_to(ROOT)),
            "profiles_path": str(args.profiles) if args.profiles else None,
            "models": [profile_to_json(profile) for profile in profiles],
            "baseline": args.baseline,
            "seed": args.seed,
        },
    )

    if args.command == "semantic":
        if args.cases_file is not None:
            cases = load_jsonl(args.cases_file)
        else:
            cases = collect_semantic_cases(
                limit=args.limit,
                guides=args.guides,
                places=args.places,
                include_noted=args.include_noted,
                fixture_suite=args.fixture_suite,
            )
        write_jsonl(run_dir / "semantic-cases.jsonl", cases)
        if args.capture_only:
            print(f"Wrote semantic cases to {run_dir / 'semantic-cases.jsonl'}")
            return 0
        records = run_semantic_eval(
            cases,
            profiles=profiles,
            run_dir=run_dir,
            timeout_seconds=args.timeout_seconds,
            force=args.force,
        )
        write_judge_pack(
            run_dir / "judge-pack.md",
            task="semantic",
            records=records,
            baseline=args.baseline,
        )
    elif args.command == "dom-repair":
        if args.cases_file is not None:
            cases = load_jsonl(args.cases_file)
        else:
            cases = collect_dom_repair_cases(
                limit=args.limit,
                guides=args.guides,
                places=args.places,
                headless=not args.headful,
                collect_reviews=args.collect_reviews,
                collect_about=args.collect_about,
            )
        write_jsonl(run_dir / "dom-repair-cases.jsonl", cases)
        if args.capture_only:
            print(f"Wrote DOM repair cases to {run_dir / 'dom-repair-cases.jsonl'}")
            return 0
        records = run_dom_repair_eval(
            cases,
            profiles=profiles,
            run_dir=run_dir,
            timeout_seconds=args.timeout_seconds,
            force=args.force,
        )
        write_judge_pack(
            run_dir / "judge-pack.md",
            task="dom-repair",
            records=records,
            baseline=args.baseline,
        )
    else:
        raise AssertionError(args.command)

    print(f"Wrote eval run to {run_dir}")
    return 0


def resolve_model_profiles(
    names_csv: str,
    *,
    profiles_path: Path | None,
    fallback_base_url: str | None,
    fallback_api_key_env: str | None = None,
) -> list[ModelProfile]:
    available = load_model_profiles(DEFAULT_MODEL_PROFILES_PATH)
    if profiles_path is not None:
        available.update(load_model_profiles(profiles_path))
    profiles: list[ModelProfile] = []
    for raw_name in names_csv.split(","):
        name = raw_name.strip()
        if not name:
            continue
        profile = available.get(name)
        if profile is None and name.startswith("openrouter:"):
            profile = ModelProfile(
                name=name,
                provider="openai-compatible",
                model=name.removeprefix("openrouter:"),
                api_key_env="OPENROUTER_API_KEY",
                base_url="https://openrouter.ai/api/v1",
                request_options={"response_format": {"type": "json_object"}},
            )
        if profile is None:
            api_key_env = fallback_api_key_env or ("OPENAI_API_KEY" if not fallback_base_url else "LLM_API_KEY")
            profile = ModelProfile(
                name=name,
                provider="openai-compatible",
                model=name,
                api_key_env=api_key_env,
                base_url=fallback_base_url or "https://api.openai.com/v1",
                request_options={"response_format": {"type": "json_object"}},
            )
        profiles.append(profile)
    if not profiles:
        raise RuntimeError("At least one model is required.")
    return profiles


def load_model_profiles(path: Path) -> dict[str, ModelProfile]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise RuntimeError(f"{path} must contain a JSON object.")
    profiles_payload = payload.get("models", payload)
    if not isinstance(profiles_payload, Mapping):
        raise RuntimeError(f"{path} models must contain a JSON object.")
    profiles: dict[str, ModelProfile] = {}
    for name, value in profiles_payload.items():
        if not isinstance(name, str) or not isinstance(value, Mapping):
            continue
        provider = string_value(value.get("provider")) or "openai-compatible"
        if provider not in ("openai-compatible", "anthropic"):
            raise RuntimeError(f"{path} profile {name} has unsupported provider {provider}.")
        profiles[name] = ModelProfile(
            name=name,
            provider=provider,
            model=string_value(value.get("model")) or name,
            api_key_env=string_value(value.get("api_key_env")) or "OPENAI_API_KEY",
            base_url=string_value(value.get("base_url")),
            request_options=dict_value(value.get("request_options")),
            cost_per_1m=float_dict_value(value.get("cost_per_1m")),
        )
    return profiles


def load_profile_overrides(path: Path) -> dict[str, ModelProfile]:
    return load_model_profiles(path)


def collect_semantic_cases(
    *,
    limit: int,
    guides: Sequence[str] | None,
    places: Sequence[str] | None,
    include_noted: bool,
    fixture_suite: Literal["random", "stratified"] = "stratified",
) -> list[dict[str, Any]]:
    entries = load_sqlite_cache_rows()
    cases: list[dict[str, Any]] = []
    allowed_guides = set(guides or [])
    selectors = [selector.casefold() for selector in places or []]
    for raw_path in sorted(build_data.RAW_DIR.glob("*.json")):
        slug = raw_path.stem
        if allowed_guides and slug not in allowed_guides:
            continue
        raw = RawSavedList.model_validate_json(raw_path.read_text(encoding="utf-8"))
        city_name, country_name = build_data.guide_location_context(slug, raw)
        for raw_place in raw.places:
            place_id = build_data.stable_place_id(raw_place, source_type=raw.configured_source_type)
            if selectors and not selector_matches(slug, place_id, raw_place.name, selectors):
                continue
            if raw_place.note and not include_noted:
                continue
            entry = entries.get((slug, place_id))
            if entry is None or entry.place is None or entry.error:
                continue
            labels = semantic_case_labels(
                raw_place=raw_place,
                enrichment=entry.place,
                city_name=city_name,
                country_name=country_name,
            )
            evidence = build_data.semantic_enrichment_evidence(
                entry.place,
                raw_place=raw_place,
                city_name=city_name,
                country_name=country_name,
                raw_note=raw_place.note,
                include_review_signals=True,
                include_raw_note=True,
            )
            evidence["requested_outputs"] = {
                "semantic_tags": True,
                "description": True,
            }
            cases.append(
                {
                    "case_id": f"{slug}:{place_id}",
                    "guide_slug": slug,
                    "place_id": place_id,
                    "place_name": raw_place.name,
                    "city": city_name,
                    "country": country_name,
                    "raw_place": raw_place.model_dump(mode="json", exclude_none=True),
                    "current_enrichment": entry.place.model_dump(mode="json", exclude_none=True),
                    "evidence": evidence,
                    "fixture_labels": labels,
                    "current_semantic": {
                        "neighborhood": entry.place.semantic_neighborhood,
                        "tags": entry.place.semantic_tags,
                        "vibe_tags": entry.place.semantic_vibe_tags,
                        "types": entry.place.semantic_types,
                        "description": entry.place.semantic_description,
                    },
                }
            )
    if fixture_suite == "random":
        random.shuffle(cases)
        return cases[:limit]
    return select_stratified_semantic_cases(cases, limit=limit)


def semantic_case_labels(
    *,
    raw_place: RawPlace,
    enrichment: Any,
    city_name: str | None,
    country_name: str | None,
) -> list[str]:
    labels: list[str] = []
    evidence_labels = semantic_evidence_shape_labels(raw_place=raw_place, enrichment=enrichment)
    labels.extend(f"evidence:{label}" for label in evidence_labels)
    place_type = semantic_place_type_label(enrichment)
    if place_type:
        labels.append(f"type:{place_type}")
    geography = semantic_geography_label(country_name=country_name)
    if geography:
        labels.append(f"geo:{geography}")
    if raw_place.name and any(not char.isascii() for char in raw_place.name):
        labels.append("name:non-english")
    if city_name:
        labels.append(f"city:{build_data.normalize_tag_slug(city_name)}")
    return labels


def semantic_evidence_shape_labels(*, raw_place: RawPlace, enrichment: Any) -> list[str]:
    labels: list[str] = []
    has_google_description = bool(string_value(getattr(enrichment, "description", None)))
    has_search_description = bool(string_value(getattr(enrichment, "search_result_description", None)))
    has_review_topics = bool(getattr(enrichment, "review_topics", None))
    has_reviews = bool(getattr(enrichment, "reviews", None))
    has_about = bool(getattr(enrichment, "about_sections", None))
    has_note = bool(string_value(raw_place.note))
    if has_note:
        labels.append("raw-note")
    if has_google_description:
        labels.append("google-description")
    if has_search_description:
        labels.append("search-description")
    if has_review_topics:
        labels.append("review-topics")
    if has_reviews:
        labels.append("review-signals")
    if has_about:
        labels.append("about-sections")
    if not any((has_note, has_google_description, has_search_description, has_review_topics, has_reviews, has_about)):
        labels.append("sparse")
    return labels


def semantic_place_type_label(enrichment: Any) -> str | None:
    category = build_data.normalize_tag_slug(string_value(getattr(enrichment, "primary_type_display_name", None)) or "")
    types = {
        build_data.normalize_tag_slug(value)
        for value in getattr(enrichment, "types", [])
        if isinstance(value, str)
    }
    text = " ".join([category, *sorted(types)])
    if any(token in text for token in ("cafe", "coffee", "bakery", "tea")):
        return "cafe"
    if any(token in text for token in ("restaurant", "food", "meal", "pizza", "noodle")):
        return "food"
    if any(token in text for token in ("bar", "night", "wine", "pub", "lounge")):
        return "bar-nightlife"
    if any(token in text for token in ("museum", "gallery", "tourist", "landmark", "park")):
        return "culture-attraction"
    if any(token in text for token in ("hotel", "lodging", "spa", "bath")):
        return "lodging-wellness"
    if any(token in text for token in ("shop", "store", "market", "mall")):
        return "shopping"
    if any(token in text for token in ("transit", "bus", "train", "government", "office")):
        return "service-transit"
    return category or None


def semantic_geography_label(*, country_name: str | None) -> str | None:
    country_key = build_data.normalize_tag_slug(country_name or "")
    if country_key in {
        "japan",
        "taiwan",
        "korea",
        "south-korea",
        "singapore",
        "thailand",
        "indonesia",
        "malaysia",
        "vietnam",
        "hong-kong",
        "china",
    }:
        return "asia"
    if country_key in {
        "italy",
        "spain",
        "france",
        "netherlands",
        "united-kingdom",
        "uk",
        "germany",
        "switzerland",
        "luxembourg",
        "ireland",
        "portugal",
        "hungary",
        "monaco",
    }:
        return "europe"
    if country_key in {"australia", "new-zealand"}:
        return "australia-nz"
    if country_key in {"usa", "united-states", "canada"}:
        return "north-america"
    if country_key in {"peru", "mexico", "brazil", "argentina"}:
        return "latin-america"
    if country_key in {"kazakhstan", "united-arab-emirates", "uae", "turkey"}:
        return "central-asia-middle-east"
    if country_key in {"south-africa"}:
        return "africa"
    if country_key in {"fiji"}:
        return "pacific-islands"
    return country_key or None


def select_stratified_semantic_cases(cases: Sequence[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    max_sparse = max(3, min(6, limit // 4))
    targets = [
        "evidence:sparse",
        "evidence:sparse",
        "evidence:sparse",
        "evidence:sparse",
        "evidence:raw-note",
        "evidence:google-description",
        "evidence:search-description",
        "evidence:review-topics",
        "evidence:review-signals",
        "evidence:about-sections",
        "name:non-english",
        "type:food",
        "type:cafe",
        "type:bar-nightlife",
        "type:culture-attraction",
        "type:lodging-wellness",
        "type:shopping",
        "type:service-transit",
        "geo:asia",
        "geo:europe",
        "geo:australia-nz",
        "geo:north-america",
        "geo:latin-america",
        "geo:central-asia-middle-east",
    ]
    shuffled = list(cases)
    random.shuffle(shuffled)
    selected: list[dict[str, Any]] = []
    selected_ids: set[str] = set()
    covered: set[str] = set()
    sparse_count = 0

    for target in targets:
        candidate = best_semantic_case_for_target(
            shuffled,
            target=target,
            selected_ids=selected_ids,
            covered=covered,
            max_sparse=max_sparse,
            sparse_count=sparse_count,
        )
        if candidate is None:
            continue
        append_semantic_case_selection(selected, selected_ids, covered, candidate)
        if semantic_case_is_sparse(candidate):
            sparse_count += 1
        if len(selected) >= limit:
            return selected

    while len(selected) < limit:
        candidate = best_semantic_case_for_target(
            shuffled,
            target=None,
            selected_ids=selected_ids,
            covered=covered,
            max_sparse=max_sparse,
            sparse_count=sparse_count,
        )
        if candidate is None:
            break
        append_semantic_case_selection(selected, selected_ids, covered, candidate)
        if semantic_case_is_sparse(candidate):
            sparse_count += 1
    return selected


def best_semantic_case_for_target(
    cases: Sequence[dict[str, Any]],
    *,
    target: str | None,
    selected_ids: set[str],
    covered: set[str],
    max_sparse: int,
    sparse_count: int,
) -> dict[str, Any] | None:
    best_case: dict[str, Any] | None = None
    best_score = -1
    for case in cases:
        case_id = string_value(case.get("case_id"))
        if case_id is None or case_id in selected_ids:
            continue
        labels = set(semantic_fixture_labels(case))
        is_sparse = "evidence:sparse" in labels
        if is_sparse and sparse_count >= max_sparse:
            continue
        if target is not None and target not in labels:
            continue
        score = len(labels - covered)
        if is_sparse:
            score += 2 if target == "evidence:sparse" else -6
        if "evidence:raw-note" in labels or "evidence:google-description" in labels:
            score += 3
        if "evidence:review-topics" in labels or "evidence:about-sections" in labels:
            score += 2
        if target is not None and not target.startswith("evidence:") and not is_sparse:
            score += 3
        if score > best_score:
            best_case = case
            best_score = score
    return best_case


def append_semantic_case_selection(
    selected: list[dict[str, Any]],
    selected_ids: set[str],
    covered: set[str],
    case: dict[str, Any],
) -> None:
    case_id = string_value(case.get("case_id"))
    if case_id is None:
        return
    selected.append(case)
    selected_ids.add(case_id)
    covered.update(semantic_fixture_labels(case))


def semantic_case_is_sparse(case: Mapping[str, Any]) -> bool:
    return "evidence:sparse" in semantic_fixture_labels(case)


def semantic_fixture_labels(case: Mapping[str, Any]) -> list[str]:
    labels = case.get("fixture_labels")
    if not isinstance(labels, list):
        return []
    return [label for label in labels if isinstance(label, str)]


def collect_dom_repair_cases(
    *,
    limit: int,
    guides: Sequence[str] | None,
    places: Sequence[str] | None,
    headless: bool,
    collect_reviews: bool,
    collect_about: bool,
) -> list[dict[str, Any]]:
    if scrape_place is None:
        raise RuntimeError("gmaps-scraper is not available.")
    cases: list[dict[str, Any]] = []
    allowed_guides = set(guides or [])
    selectors = [selector.casefold() for selector in places or []]
    proxy = build_data.current_scraper_proxy()
    session_state = build_data.ensure_scraper_session_state(proxy, session_scope="llm-eval-dom-repair")
    browser_session, http_session = build_data.build_scraper_configs(session_state, proxy)
    try:
        for raw_path in sorted(build_data.RAW_DIR.glob("*.json")):
            slug = raw_path.stem
            if allowed_guides and slug not in allowed_guides:
                continue
            raw = RawSavedList.model_validate_json(raw_path.read_text(encoding="utf-8"))
            city_name, country_name = build_data.guide_location_context(slug, raw)
            for raw_place in raw.places:
                place_id = build_data.stable_place_id(raw_place, source_type=raw.configured_source_type)
                if selectors and not selector_matches(slug, place_id, raw_place.name, selectors):
                    continue
                urls = build_data.build_place_page_candidate_urls(
                    raw_place,
                    city_name=city_name,
                    country_name=country_name,
                )
                if not urls:
                    continue
                request_box: dict[str, Any] = {}

                def capture_request(request: PlaceLLMRepairRequest) -> None:
                    request_box["request"] = request.to_dict()
                    return None

                try:
                    details = scrape_place(
                        urls[0],
                        headless=headless,
                        browser_session=browser_session,
                        http_session=http_session,
                        llm_fallback=capture_request,
                        llm_policy="always",
                        llm_tasks=("dom_repair",),
                        collect_reviews=collect_reviews,
                        collect_about=collect_about,
                    )
                except Exception as exc:
                    cases.append(
                        {
                            "case_id": f"{slug}:{place_id}",
                            "guide_slug": slug,
                            "place_id": place_id,
                            "place_name": raw_place.name,
                            "source_url": urls[0],
                            "error": str(exc),
                        }
                    )
                    if len(cases) >= limit:
                        return cases
                    continue
                build_data.record_scraper_session_use(session_state, proxy=proxy)
                request_payload = request_box.get("request")
                if not isinstance(request_payload, dict):
                    continue
                cases.append(
                    {
                        "case_id": f"{slug}:{place_id}",
                        "guide_slug": slug,
                        "place_id": place_id,
                        "place_name": raw_place.name,
                        "source_url": urls[0],
                        "details_without_llm": details.to_dict(),
                        "repair_request": request_payload,
                    }
                )
                if len(cases) >= limit:
                    return cases
    finally:
        build_data.release_scraper_session_lock(session_state)
    return cases


def run_semantic_eval(
    cases: Sequence[dict[str, Any]],
    *,
    profiles: Sequence[ModelProfile],
    run_dir: Path,
    timeout_seconds: float,
    force: bool,
) -> list[dict[str, Any]]:
    cases_path = run_dir / "semantic-cases.jsonl"
    write_jsonl(cases_path, cases)
    records: list[dict[str, Any]] = []
    for case in cases:
        outputs: dict[str, Any] = {}
        for profile in profiles:
            outputs[profile.name] = cached_or_call(
                run_dir=run_dir,
                task="semantic",
                case_id=case["case_id"],
                profile=profile,
                force=force,
                call=lambda profile=profile, case=case: call_semantic_model(
                    profile,
                    case,
                    timeout_seconds=timeout_seconds,
                ),
            )
        records.append({**case, "outputs": outputs})
    write_jsonl(run_dir / "semantic-results.jsonl", records)
    return records


def run_dom_repair_eval(
    cases: Sequence[dict[str, Any]],
    *,
    profiles: Sequence[ModelProfile],
    run_dir: Path,
    timeout_seconds: float,
    force: bool,
) -> list[dict[str, Any]]:
    cases_path = run_dir / "dom-repair-cases.jsonl"
    write_jsonl(cases_path, cases)
    records: list[dict[str, Any]] = []
    for case in cases:
        outputs: dict[str, Any] = {}
        if "repair_request" not in case:
            records.append({**case, "outputs": outputs})
            continue
        for profile in profiles:
            outputs[profile.name] = cached_or_call(
                run_dir=run_dir,
                task="dom-repair",
                case_id=case["case_id"],
                profile=profile,
                force=force,
                call=lambda profile=profile, case=case: call_dom_repair_model(
                    profile,
                    case,
                    timeout_seconds=timeout_seconds,
                ),
            )
        records.append({**case, "outputs": outputs})
    write_jsonl(run_dir / "dom-repair-results.jsonl", records)
    return records


def call_semantic_model(
    profile: ModelProfile,
    case: Mapping[str, Any],
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    started = time.monotonic()
    response = call_json_model(
        profile,
        system=build_data.SEMANTIC_ENRICHMENT_SYSTEM_PROMPT,
        user_payload={
            "schema": {
                "neighborhood": "string or null",
                "tags": ["major visible/search tags, kebab-case"],
                "vibe_tags": ["atmosphere/use-case tags, kebab-case"],
                "types": ["specific place type tags, kebab-case"],
                "description": "one concise sentence or null",
            },
            "evidence": case["evidence"],
        },
        timeout_seconds=timeout_seconds,
    )
    decoded = response["json"] if isinstance(response.get("json"), dict) else {}
    enrichment_place = build_data.EnrichmentPlace.model_validate(case["current_enrichment"])
    raw_place = RawPlace.model_validate(case["raw_place"])
    accepted_description = build_data.usable_semantic_description(
        decoded.get("description"),
        enrichment_place=enrichment_place,
        raw_place=raw_place,
        city_name=string_value(case.get("city")),
        country_name=string_value(case.get("country")),
    )
    return {
        "model": profile.name,
        "status": "ok",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "usage": response.get("usage"),
        "estimated_cost_usd": estimated_response_cost_usd(profile, response.get("usage")),
        "raw": decoded,
        "accepted": {
            "neighborhood": build_data.normalize_semantic_neighborhood_label(
                decoded.get("neighborhood"),
                city_name=string_value(case.get("city")),
                country_name=string_value(case.get("country")),
            ),
            "tags": build_data.normalize_semantic_tag_list(decoded.get("tags"), limit=8),
            "vibe_tags": build_data.normalize_semantic_tag_list(decoded.get("vibe_tags"), limit=8),
            "types": build_data.normalize_semantic_tag_list(decoded.get("types"), limit=8),
            "description": accepted_description,
        },
    }


def call_dom_repair_model(
    profile: ModelProfile,
    case: Mapping[str, Any],
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    started = time.monotonic()
    request_payload = case["repair_request"]
    tasks = request_payload.get("tasks")
    response = call_json_model(
        profile,
        system=(
            "You repair Google Maps place extraction from sanitized DOM evidence. "
            "Return only JSON. Only include fields directly supported by evidence. "
            "Do not infer product-specific tags, neighborhoods, or marketing prose. "
            "Never translate review topic labels. Do not return user-generated review text."
        ),
        user_payload={
            "allowed_fields": allowed_dom_repair_fields(tasks if isinstance(tasks, list) else []),
            "review_topic_shape": {"label": "string", "count": "integer or null"},
            "about_section_shape": {
                "title": "string",
                "items": [{"label": "string", "aria_label": "string or null"}],
            },
            "request": request_payload,
        },
        timeout_seconds=timeout_seconds,
    )
    decoded = response["json"] if isinstance(response.get("json"), dict) else {}
    fields = decoded.get("fields") if isinstance(decoded.get("fields"), dict) else decoded
    return {
        "model": profile.name,
        "status": "ok",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "usage": response.get("usage"),
        "estimated_cost_usd": estimated_response_cost_usd(profile, response.get("usage")),
        "raw": decoded,
        "fields": fields if isinstance(fields, dict) else {},
    }


def call_json_model(
    profile: ModelProfile,
    *,
    system: str,
    user_payload: Mapping[str, Any],
    timeout_seconds: float,
) -> dict[str, Any]:
    if profile.provider == "anthropic":
        return call_anthropic_json_model(
            profile,
            system=system,
            user_payload=user_payload,
            timeout_seconds=timeout_seconds,
        )
    return call_openai_compatible_json_model(
        profile,
        system=system,
        user_payload=user_payload,
        timeout_seconds=timeout_seconds,
    )


def call_openai_compatible_json_model(
    profile: ModelProfile,
    *,
    system: str,
    user_payload: Mapping[str, Any],
    timeout_seconds: float,
) -> dict[str, Any]:
    if not profile.base_url:
        raise RuntimeError(f"Model profile {profile.name} is missing base_url.")
    validate_api_url(profile.base_url, f"Model profile {profile.name}")
    payload: dict[str, Any] = {
        "model": profile.model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
        ],
    }
    payload.update(profile.request_options)
    request = Request(
        f"{profile.base_url.rstrip('/')}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {profile.api_key()}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    response_payload = read_json_response(request, timeout_seconds=timeout_seconds)
    content = extract_openai_message_content(response_payload)
    return {
        "json": decode_json_object(content),
        "usage": response_payload.get("usage") if isinstance(response_payload, dict) else None,
    }


def call_anthropic_json_model(
    profile: ModelProfile,
    *,
    system: str,
    user_payload: Mapping[str, Any],
    timeout_seconds: float,
) -> dict[str, Any]:
    base_url = profile.base_url or "https://api.anthropic.com"
    validate_api_url(base_url, f"Model profile {profile.name}")
    payload: dict[str, Any] = {
        "model": profile.model,
        "max_tokens": 1200,
        "system": f"{system}\nReturn only a single JSON object.",
        "messages": [
            {
                "role": "user",
                "content": json.dumps(user_payload, ensure_ascii=False),
            }
        ],
    }
    payload.update(profile.request_options)
    payload["max_tokens"] = int(payload.get("max_tokens", 1200))
    request = Request(
        f"{base_url.rstrip('/')}/v1/messages",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "x-api-key": profile.api_key(),
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    response_payload = read_json_response(request, timeout_seconds=timeout_seconds)
    content = extract_anthropic_message_content(response_payload)
    return {
        "json": decode_json_object(content),
        "usage": response_payload.get("usage") if isinstance(response_payload, dict) else None,
    }


def validate_api_url(url: str, context: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise RuntimeError(f"{context}: API URL must use http:// or https://, got {url!r}.")


def read_json_response(request: Request, *, timeout_seconds: float) -> dict[str, Any]:
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Model HTTP {exc.code}: {body[:1000]}") from exc
    except (URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Model request failed: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("Model response must be a JSON object.")
    return payload


def cached_or_call(
    *,
    run_dir: Path,
    task: str,
    case_id: str,
    profile: ModelProfile,
    force: bool,
    call: Any,
) -> dict[str, Any]:
    cache_dir = run_dir / "responses" / task / safe_filename(profile.name)
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"{safe_filename(case_id)}.json"
    if not force:
        cached = read_optional_json(cache_path)
        if isinstance(cached, dict):
            return cached
    try:
        result = call()
    except Exception as exc:
        result = {
            "model": profile.name,
            "status": "error",
            "error": str(exc),
        }
    write_json(cache_path, result)
    return result


def write_judge_pack(
    path: Path,
    *,
    task: Literal["semantic", "dom-repair"],
    records: Sequence[Mapping[str, Any]],
    baseline: str,
) -> None:
    lines = [
        f"# LLM {task} Judge Pack",
        "",
        "Use this to compare candidates against the baseline or paste individual cases into this Codex GPT-5.5 chat.",
        "",
    ]
    if task == "semantic":
        lines.extend(
            [
                "Criteria: factual grounding, concise travel-guide tone, accurate neighborhood/category/type signal, useful reusable tags, no review-source leakage, no hype.",
                "",
            ]
        )
    else:
        lines.extend(
            [
                "Criteria: fields are supported by DOM evidence, no invented metadata, no user review text, useful repair of missing/rejected fields, valid compact JSON.",
                "",
            ]
        )
    for index, record in enumerate(records, start=1):
        outputs = record.get("outputs") if isinstance(record.get("outputs"), Mapping) else {}
        lines.extend(
            [
                f"## Case {index}: {record.get('case_id')}",
                "",
                f"Place: {record.get('place_name')}",
                f"Guide: {record.get('guide_slug')}",
                "",
            ]
        )
        if task == "semantic":
            labels = record.get("fixture_labels")
            if labels:
                lines.extend(["Fixture labels:", fenced_json(labels), ""])
            lines.extend(
                [
                    "Evidence:",
                    fenced_json(compact_semantic_evidence(record.get("evidence"))),
                    "",
                    "Existing cached semantic output:",
                    fenced_json(record.get("current_semantic")),
                    "",
                ]
            )
        else:
            repair_request = record.get("repair_request")
            if repair_request:
                lines.extend(
                    [
                        "Current fields:",
                        fenced_json(repair_request.get("current_fields") if isinstance(repair_request, dict) else None),
                        "",
                        "Quality flags:",
                        fenced_json(
                            repair_request.get("diagnostics", {}).get("quality_flags")
                            if isinstance(repair_request, dict)
                            else None
                        ),
                        "",
                        "Evidence:",
                        fenced_json(repair_request.get("evidence") if isinstance(repair_request, dict) else None),
                        "",
                    ]
                )
            elif record.get("error"):
                lines.extend(["Capture error:", str(record.get("error")), ""])
        ordered_model_names = sorted(outputs)
        if baseline in outputs:
            ordered_model_names = [baseline, *[name for name in ordered_model_names if name != baseline]]
        for model_name in ordered_model_names:
            output = outputs.get(model_name)
            lines.extend(
                [
                    f"### {model_name}",
                    "",
                    fenced_json(output),
                    "",
                ]
            )
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def compact_semantic_evidence(value: Any) -> Any:
    if not isinstance(value, Mapping):
        return value
    keys = [
        "city",
        "country",
        "name",
        "address",
        "category",
        "category_localized",
        "google_maps_description",
        "search_result_description",
        "types",
        "price_range",
        "review_topics",
        "about_sections",
        "review_signals",
        "raw_note",
    ]
    return {key: value.get(key) for key in keys if value.get(key) not in (None, "", [])}


def load_sqlite_cache_rows() -> dict[tuple[str, str], EnrichmentCacheEntry]:
    if not build_data.PLACES_SQLITE_PATH.exists():
        return {}
    connection = sqlite3.connect(build_data.PLACES_SQLITE_PATH)
    try:
        rows = connection.execute(
            """
            SELECT guide_slug, place_id, cache_json
            FROM guide_enrichment_cache
            ORDER BY guide_slug, place_id
            """
        ).fetchall()
    finally:
        connection.close()
    entries: dict[tuple[str, str], EnrichmentCacheEntry] = {}
    for guide_slug, place_id, cache_json in rows:
        if isinstance(guide_slug, str) and isinstance(place_id, str) and isinstance(cache_json, str):
            entries[(guide_slug, place_id)] = EnrichmentCacheEntry.model_validate_json(cache_json)
    return entries


def allowed_dom_repair_fields(tasks: Sequence[str]) -> list[str]:
    allowed: list[str] = []
    if "dom_repair" in tasks:
        allowed.extend(PLACE_LLM_DOM_REPAIR_FIELDS)
    if "display_translation" in tasks:
        allowed.extend(PLACE_LLM_DISPLAY_TRANSLATION_FIELDS)
    return list(dict.fromkeys(allowed or [*PLACE_LLM_DOM_REPAIR_FIELDS, *PLACE_LLM_DISPLAY_TRANSLATION_FIELDS]))


def estimated_response_cost_usd(profile: ModelProfile, usage: Any) -> float | None:
    if not profile.cost_per_1m or not isinstance(usage, Mapping):
        return None
    input_tokens = int_value(usage.get("prompt_tokens") or usage.get("input_tokens")) or 0
    output_tokens = int_value(usage.get("completion_tokens") or usage.get("output_tokens")) or 0
    cached_tokens = cached_input_tokens(usage)
    billable_input_tokens = max(0, input_tokens - cached_tokens)
    cost = (
        billable_input_tokens * profile.cost_per_1m.get("input", 0.0)
        + cached_tokens * profile.cost_per_1m.get("cached", profile.cost_per_1m.get("input", 0.0))
        + output_tokens * profile.cost_per_1m.get("output", 0.0)
    ) / 1_000_000
    return round(cost, 8)


def cached_input_tokens(usage: Mapping[str, Any]) -> int:
    details = usage.get("prompt_tokens_details") or usage.get("input_tokens_details")
    if not isinstance(details, Mapping):
        return 0
    return int_value(details.get("cached_tokens")) or 0


def extract_openai_message_content(payload: Mapping[str, Any]) -> str:
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("Model response is missing choices.")
    first = choices[0]
    if not isinstance(first, Mapping):
        raise RuntimeError("Model response choice is not an object.")
    message = first.get("message")
    if not isinstance(message, Mapping):
        raise RuntimeError("Model response choice is missing message.")
    content = message.get("content")
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("Model response message has no text content.")
    return content


def extract_anthropic_message_content(payload: Mapping[str, Any]) -> str:
    content = payload.get("content")
    if not isinstance(content, list):
        raise RuntimeError("Anthropic response is missing content.")
    parts: list[str] = []
    for item in content:
        if isinstance(item, Mapping) and item.get("type") == "text" and isinstance(item.get("text"), str):
            parts.append(item["text"])
    text = "\n".join(parts).strip()
    if not text:
        raise RuntimeError("Anthropic response has no text content.")
    return text


def decode_json_object(content: str) -> dict[str, Any]:
    stripped = content.strip()
    if stripped.startswith("```"):
        stripped = stripped.removeprefix("```json").removeprefix("```").strip()
        stripped = stripped.removesuffix("```").strip()
    try:
        decoded = json.loads(stripped)
    except json.JSONDecodeError:
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start < 0 or end <= start:
            raise
        decoded = json.loads(stripped[start : end + 1])
    if not isinstance(decoded, dict):
        raise RuntimeError("Model content must decode to a JSON object.")
    return decoded


def selector_matches(slug: str, place_id: str, name: str, selectors: Sequence[str]) -> bool:
    haystack = f"{slug} {place_id} {name}".casefold()
    return any(selector in haystack for selector in selectors)


def build_run_dir(output_root: Path, run_name: str | None, command: str) -> Path:
    if run_name:
        return output_root / run_name
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return output_root / f"{timestamp}-{command}"


def safe_filename(value: str) -> str:
    return "".join(char if char.isalnum() or char in "-._" else "-" for char in value)[:180]


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            payload = json.loads(stripped)
            if not isinstance(payload, dict):
                raise RuntimeError(f"{path}:{line_number} must contain a JSON object.")
            rows.append(payload)
    return rows


def read_optional_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def fenced_json(value: Any) -> str:
    return "```json\n" + json.dumps(value, indent=2, ensure_ascii=False) + "\n```"


def profile_to_json(profile: ModelProfile) -> dict[str, Any]:
    return {
        "name": profile.name,
        "provider": profile.provider,
        "model": profile.model,
        "api_key_env": profile.api_key_env,
        "base_url": profile.base_url,
        "request_options": profile.request_options,
        "cost_per_1m": profile.cost_per_1m,
    }


def load_dotenv_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return values
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, raw_value = stripped.split("=", 1)
        values[key.strip()] = raw_value.strip().strip("\"'")
    return values


def string_value(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def int_value(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def dict_value(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def float_dict_value(value: Any) -> dict[str, float]:
    if not isinstance(value, Mapping):
        return {}
    result: dict[str, float] = {}
    for key, raw in value.items():
        if isinstance(key, str) and isinstance(raw, int | float):
            result[key] = float(raw)
    return result


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


if __name__ == "__main__":
    sys.exit(main())
