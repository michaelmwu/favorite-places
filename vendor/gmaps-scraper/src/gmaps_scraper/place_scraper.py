"""Browser-backed scraper for individual Google Maps place pages."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from typing import Any, cast
from urllib.parse import parse_qs, unquote, urlparse

from gmaps_scraper.models import PlaceDetails
from gmaps_scraper.scraper import (
    _HTTP_IMPERSONATE,
    BrowserSessionConfig,
    HttpSessionConfig,
    ScrapeError,
    _extract_preloaded_fetch_url,
    _handle_google_consent,
    _import_curl_requests,
    _launch_browser_context,
    _load_http_cookie_jar,
    _normalize_response_url,
    _raise_for_status,
    _response_text,
    _save_http_cookie_jar,
)

_TITLE_SELECTORS = ("h1.DUwDvf", "h1.lfPIob", "div[role='main'] h1")
_TITLE_SELECTOR = ", ".join(_TITLE_SELECTORS)
_REVIEW_LABEL_KEYWORDS = ("review", "reviews", "評論", "クチコミ")
_DESCRIPTION_STOP_MARKERS = {
    "photos",
    "about this data",
    "write a review",
    "claim this business",
    "suggest an edit",
    "limited view of google maps",
    "get the most out of google maps",
}
_CATEGORY_SUFFIX_PATTERN = re.compile(
    r"\b("
    r"restaurant|cafe|coffee shop|bar|bakery|hotel|lodging|museum|park|station|"
    r"store|shop|supermarket|market|mall|school|university|gym|spa|clinic|"
    r"hospital|pharmacy|library|church|temple|shrine|tourist attraction|"
    r"movie theater|fast food restaurant|ramen restaurant|sushi restaurant"
    r")\b$",
    re.IGNORECASE,
)
_PLUS_CODE_PATTERN = re.compile(
    r"\b[23456789CFGHJMPQRVWX]{4,8}\+[23456789CFGHJMPQRVWX]{2,3}"
    r"(?:\s+[^\n]+)?\b"
)
_PHONE_PATTERN = re.compile(r"^\+?[0-9][0-9()\-\s]{7,}$")
_POSTAL_CODE_PATTERN = re.compile(
    r"\b(?:\d{5}(?:-\d{4})?|[A-Z]\d[A-Z]\s?\d[A-Z]\d|[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2})\b",
    re.IGNORECASE,
)
_ADDRESS_KEYWORD_PATTERN = re.compile(
    r"\b(?:street|st|avenue|ave|road|rd|boulevard|blvd|lane|ln|drive|dr|way|place|pl|"
    r"court|ct|square|sq|suite|ste|unit|floor|fl|plaza|parkway|pkwy|highway|hwy)\b",
    re.IGNORECASE,
)
_PLACE_JS_EXTRACTOR = r"""
() => {
  const titleSelectors = ["h1.DUwDvf", "h1.lfPIob", "div[role='main'] h1"];
  let titleElement = null;
  for (const selector of titleSelectors) {
    const element = document.querySelector(selector);
    if (element?.innerText?.trim()) {
      titleElement = element;
      break;
    }
  }

  let panel = document.body;
  if (titleElement) {
    let current = titleElement;
    for (let i = 0; i < 8; i += 1) {
      if (!current.parentElement || current.parentElement.tagName === "BODY") {
        break;
      }
      current = current.parentElement;
    }
    panel = current;
  }

  const firstText = (selectors, root = panel) => {
    for (const selector of selectors) {
      const element = root.querySelector(selector);
      const text = element?.innerText?.trim();
      if (text) {
        return text;
      }
    }
    return null;
  };

  const firstAttr = (selectors, attr, root = panel) => {
    for (const selector of selectors) {
      const element = root.querySelector(selector);
      const value = element?.getAttribute(attr)?.trim();
      if (value) {
        return value;
      }
    }
    return null;
  };

  const itemValue = (itemId) => firstText([
    `[data-item-id="${itemId}"] .Io6YTe`,
    `[data-item-id="${itemId}"]`,
  ]);

  const normalizeCount = (value) => {
    if (!value) {
      return 0;
    }
    const text = value.trim().toUpperCase();
    let multiplier = 1;
    if (text.includes("K")) {
      multiplier = 1000;
    } else if (text.includes("M")) {
      multiplier = 1000000;
    } else if (text.includes("萬") || text.includes("万")) {
      multiplier = 10000;
    }
    const numeric = parseFloat(text.replace(/[,\sKM萬万]/g, ""));
    return Number.isFinite(numeric) ? numeric * multiplier : 0;
  };

  const reviewKeywords = ["review", "reviews", "評論", "クチコミ"];
  const reviewCountPattern = new RegExp(
    "([0-9][0-9,.\\s]*[KM萬万]?)[ ]*"
      + "(?:reviews?|評論|クチコミ|件のクチコミ|件の Google クチコミ|則評論|篇評論)",
    "i",
  );
  const reviewCountPatternReverse = new RegExp(
    "(?:reviews?|評論|クチコミ)\\s*[(]([0-9][0-9,.\\s]*[KM萬万]?)[)]",
    "i",
  );

  let reviewCount = null;
  let reviewSource = null;
  let bestCount = 0;

  const considerCount = (candidate, source) => {
    if (!candidate) {
      return;
    }
    const count = normalizeCount(candidate);
    if (count <= 0) {
      return;
    }
    if (count > bestCount) {
      bestCount = count;
      reviewCount = candidate.trim();
      reviewSource = source;
    }
  };

  for (const span of panel.querySelectorAll("div.F7nice span")) {
    const text = span.innerText?.trim() || "";
    const match = text.match(/^\(?([0-9][0-9,.\s]*[KM萬万]?)\)?$/i);
    if (!match) {
      continue;
    }
    if (/^[0-9]+([.,][0-9]+)?$/.test(match[1]) && normalizeCount(match[1]) < 10) {
      continue;
    }
    considerCount(match[1], "f7nice");
  }

  for (const element of panel.querySelectorAll("[aria-label]")) {
    const label = element.getAttribute("aria-label") || "";
    if (!reviewKeywords.some((keyword) => label.toLowerCase().includes(keyword.toLowerCase()))) {
      continue;
    }
    const match = label.match(reviewCountPattern) || label.match(reviewCountPatternReverse);
    if (match) {
      considerCount(match[1], "aria-label");
    }
  }

  if (!reviewCount) {
    for (const tab of panel.querySelectorAll("div[role='tablist'] button")) {
      const text = tab.innerText?.trim() || "";
      if (!reviewKeywords.some((keyword) => text.toLowerCase().includes(keyword.toLowerCase()))) {
        continue;
      }
      const match = text.match(/([0-9][0-9,.\s]*[KM萬万]?)/i);
      if (match) {
        considerCount(match[1], "tab");
      }
    }
  }

  return {
    name: firstText(titleSelectors),
    secondary_name: firstText(["h2.bwoZTb span", "h2.bwoZTb"]),
    rating: firstText([
      "div.F7nice > span > span[aria-hidden='true']:first-child",
      "span.ceNzKf[role='img']",
      "span[role='img'][aria-label*='star']",
    ]),
    review_count: reviewCount,
    review_count_source: reviewSource,
    category: firstText([
      "button[jsaction*='category']",
      ".skqShb .fontBodyMedium button",
      "button.DkEaL",
    ]),
    address: itemValue("address"),
    located_in: itemValue("locatedin"),
    status: firstText(["div.OqCZI .ZDu9vd", "div.OqCZI .o0Svhf"]),
    website: firstAttr(["a[data-item-id='authority']"], "href", document) || itemValue("authority"),
    phone: firstText([
      "button[data-item-id^='phone:'] .Io6YTe",
      "button[data-item-id^='phone:']",
    ]),
    plus_code: itemValue("oloc"),
    panel_text: panel?.innerText || "",
    body_text: document.body?.innerText || "",
    limited_view: (document.body?.innerText || "")
      .toLowerCase()
      .includes("limited view of google maps"),
  };
}
"""
_PLACE_REVIEW_SIGNAL_JS = r"""
() => {
  const titleSelectors = ["h1.DUwDvf", "h1.lfPIob", "div[role='main'] h1"];
  let titleElement = null;
  for (const selector of titleSelectors) {
    const element = document.querySelector(selector);
    if (element?.innerText?.trim()) {
      titleElement = element;
      break;
    }
  }
  let panel = document.body;
  if (titleElement) {
    let current = titleElement;
    for (let i = 0; i < 8; i += 1) {
      if (!current.parentElement || current.parentElement.tagName === "BODY") {
        break;
      }
      current = current.parentElement;
    }
    panel = current;
  }
  const f7nice = panel.querySelector("div.F7nice");
  if (f7nice?.innerText?.match(/[0-9]/)) {
    return true;
  }
  for (const element of panel.querySelectorAll("[aria-label]")) {
    const label = element.getAttribute("aria-label") || "";
    if (/(review|reviews|評論|クチコミ)/i.test(label) && /[0-9]/.test(label)) {
      return true;
    }
  }
  for (const tab of panel.querySelectorAll("div[role='tablist'] button")) {
    if (/(review|reviews|評論|クチコミ)/i.test(tab.innerText || "")) {
      return true;
    }
  }
  return false;
}
"""


def scrape_place(
    place_url: str,
    *,
    headless: bool = True,
    timeout_ms: int = 30_000,
    settle_time_ms: int = 3_000,
    browser_session: BrowserSessionConfig | None = None,
    http_session: HttpSessionConfig | None = None,
) -> PlaceDetails:
    """Scrape a Google Maps place page using a browser session."""
    snapshot = collect_place_snapshot(
        place_url,
        headless=headless,
        timeout_ms=timeout_ms,
        settle_time_ms=settle_time_ms,
        browser_session=browser_session,
        http_session=http_session,
    )
    resolved_url = _normalize_response_url(snapshot.get("resolved_url"))
    dom_snapshot = cast(Mapping[str, object], snapshot["dom"])
    preview_snapshot = cast(
        Mapping[str, object],
        snapshot.get("preview") if isinstance(snapshot.get("preview"), Mapping) else {},
    )
    merged_snapshot = _merge_place_sources(dom_snapshot, preview_snapshot)
    return _build_place_details(
        place_url,
        resolved_url=resolved_url,
        snapshot=merged_snapshot,
    )


def collect_place_snapshot(
    place_url: str,
    *,
    headless: bool,
    timeout_ms: int,
    settle_time_ms: int,
    browser_session: BrowserSessionConfig | None = None,
    http_session: HttpSessionConfig | None = None,
) -> dict[str, object]:
    """Collect a normalized DOM snapshot for a Google Maps place page."""
    context = _launch_browser_context(
        headless=headless,
        browser_session=browser_session,
    )
    try:
        page = context.new_page()
        _seed_google_consent_cookies(page, source_url=place_url)
        page.goto(place_url, wait_until="domcontentloaded", timeout=timeout_ms)
        _handle_google_consent(page, timeout_ms=timeout_ms)
        try:
            page.wait_for_load_state("load", timeout=min(timeout_ms, 10_000))
        except Exception:
            pass
        _handle_google_consent(page, timeout_ms=timeout_ms)
        try:
            page.wait_for_selector(_TITLE_SELECTOR, timeout=timeout_ms, state="attached")
        except Exception:
            pass
        _ensure_review_signal(page, timeout_ms=timeout_ms)
        page.wait_for_timeout(settle_time_ms)
        resolved_url = _normalize_response_url(getattr(page, "url", None))
        dom_snapshot = page.evaluate(_PLACE_JS_EXTRACTOR)
        preview_snapshot = _collect_preview_place_enrichment(
            place_url,
            resolved_url=resolved_url,
            timeout_ms=timeout_ms,
            http_session=http_session,
        )
    except Exception as exc:  # pragma: no cover - browser error path
        raise ScrapeError(f"Failed to scrape place page: {exc}") from exc
    finally:
        context.close()

    if not isinstance(dom_snapshot, Mapping):
        raise ScrapeError("Failed to collect a structured place snapshot from the page.")
    return {
        "resolved_url": resolved_url,
        "dom": dict(dom_snapshot),
        "preview": preview_snapshot,
    }


def _wait_for_review_signal(page: Any, *, timeout_ms: int) -> bool:
    polls = max(1, min(6, timeout_ms // 1_000))
    for _ in range(polls):
        try:
            if page.evaluate(_PLACE_REVIEW_SIGNAL_JS) is True:
                return True
        except Exception:
            pass
        page.wait_for_timeout(500)
    return False


def _ensure_review_signal(page: Any, *, timeout_ms: int) -> bool:
    if _wait_for_review_signal(page, timeout_ms=timeout_ms):
        return True

    try:
        page.reload(wait_until="domcontentloaded", timeout=timeout_ms)
    except Exception:
        return False

    _handle_google_consent(page, timeout_ms=timeout_ms)
    try:
        page.wait_for_load_state("load", timeout=min(timeout_ms, 10_000))
    except Exception:
        pass
    _handle_google_consent(page, timeout_ms=timeout_ms)
    try:
        page.wait_for_selector(_TITLE_SELECTOR, timeout=min(timeout_ms, 10_000), state="attached")
    except Exception:
        pass
    return _wait_for_review_signal(page, timeout_ms=min(timeout_ms, 4_000))


def _build_place_details(
    source_url: str,
    *,
    resolved_url: str | None,
    snapshot: Mapping[str, object],
) -> PlaceDetails:
    panel_lines = _body_lines(snapshot.get("panel_text"))
    body_lines = _body_lines(snapshot.get("body_text"))
    search_lines = panel_lines or body_lines
    combined_lines = _dedupe_lines([*panel_lines, *body_lines])
    name = _clean_text(snapshot.get("name")) or _first_nonempty(search_lines)
    category = _clean_text(snapshot.get("category")) or _extract_category_from_lines(search_lines)
    lat = _parse_float(snapshot.get("lat"))
    if lat is None:
        lat = _extract_coordinate_from_url(resolved_url or source_url, index=0)
    lng = _parse_float(snapshot.get("lng"))
    if lng is None:
        lng = _extract_coordinate_from_url(resolved_url or source_url, index=1)
    return PlaceDetails(
        source_url=source_url,
        resolved_url=resolved_url,
        name=name,
        secondary_name=_clean_text(snapshot.get("secondary_name"))
        or _extract_secondary_name(combined_lines, name=name),
        category=category,
        rating=_parse_rating(snapshot.get("rating")),
        review_count=_parse_review_count(snapshot.get("review_count")),
        address=_clean_text(snapshot.get("address")) or _extract_address_from_lines(combined_lines),
        located_in=_clean_text(snapshot.get("located_in")),
        status=_clean_text(snapshot.get("status")) or _extract_status_from_lines(combined_lines),
        website=_normalize_website(snapshot.get("website")),
        phone=_clean_text(snapshot.get("phone")) or _extract_phone_from_lines(combined_lines),
        plus_code=_clean_text(snapshot.get("plus_code"))
        or _extract_plus_code_from_lines(combined_lines),
        description=_extract_description(snapshot, combined_lines),
        lat=lat,
        lng=lng,
        limited_view=_to_bool(snapshot.get("limited_view"))
        or any("limited view of google maps" in line.lower() for line in combined_lines),
    )


def _seed_google_consent_cookies(page: Any, *, source_url: str) -> None:
    context = getattr(page, "context", None)
    add_cookies = getattr(context, "add_cookies", None)
    if not callable(add_cookies):
        return

    host = urlparse(source_url).hostname
    cookie_targets = ["https://www.google.com"]
    if isinstance(host, str) and host:
        cookie_targets.append(f"https://{host}")

    cookies = [
        {
            "name": "CONSENT",
            "value": "YES+cb.20240101-01-p0.en+FX+430",
            "url": target,
        }
        for target in cookie_targets
    ]
    try:
        add_cookies(cookies)
    except Exception:
        return


def _merge_place_sources(
    primary: Mapping[str, object],
    secondary: Mapping[str, object],
) -> dict[str, object]:
    merged = dict(primary)
    for key, value in secondary.items():
        if key == "limited_view":
            merged[key] = _to_bool(merged.get(key)) or _to_bool(value)
            continue
        if _is_missing_value(merged.get(key)) and not _is_missing_value(value):
            merged[key] = value
    return merged


def _is_missing_value(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return False


def _collect_preview_place_enrichment(
    place_url: str,
    *,
    resolved_url: str | None,
    timeout_ms: int,
    http_session: HttpSessionConfig | None = None,
) -> dict[str, object]:
    curl_requests = _import_curl_requests()
    timeout_seconds = max(timeout_ms / 1_000, 1.0)
    base_url = resolved_url or place_url
    session_kwargs: dict[str, object] = {
        "impersonate": _HTTP_IMPERSONATE,
        "allow_redirects": True,
        "default_headers": True,
        "timeout": timeout_seconds,
    }
    cookie_jar = _load_http_cookie_jar(http_session)
    if cookie_jar is not None:
        session_kwargs["cookies"] = cookie_jar
    if http_session is not None and http_session.proxy is not None:
        session_kwargs["proxy"] = http_session.proxy

    try:
        with curl_requests.Session(**session_kwargs) as session:
            page_response = session.get(base_url)
            _raise_for_status(page_response)
            page_html = _response_text(page_response)
            preload_url = _extract_preloaded_fetch_url(
                page_html,
                base_url=base_url,
                preferred_path_markers=("preview/place",),
            )
            if preload_url is None:
                return {}
            preload_response = session.get(preload_url, referer=base_url)
            _raise_for_status(preload_response)
            payload_text = _response_text(preload_response)
    except Exception:
        return {}
    finally:
        _save_http_cookie_jar(http_session, cookie_jar)

    return _extract_preview_place_enrichment(payload_text)


def _extract_preview_place_enrichment(payload_text: str) -> dict[str, object]:
    root = _load_preview_payload(payload_text)
    if not isinstance(root, list):
        return {}

    strings = [value for value in _iter_strings(root) if _is_meaningful_preview_string(value)]
    enrichment: dict[str, object] = {}

    website = _extract_preview_website(strings)
    if website is not None:
        enrichment["website"] = website

    phone = _extract_preview_phone(strings)
    if phone is not None:
        enrichment["phone"] = phone

    plus_code = _extract_preview_plus_code(strings)
    if plus_code is not None:
        enrichment["plus_code"] = plus_code

    address = _extract_preview_address(strings)
    if address is not None:
        enrichment["address"] = address

    category = _extract_preview_category(root, strings)
    if category is not None:
        enrichment["category"] = category

    description = _extract_preview_description(strings)
    if description is not None:
        enrichment["description"] = description

    coordinates = _extract_preview_coordinates(root)
    if coordinates is not None:
        enrichment["lat"] = coordinates[0]
        enrichment["lng"] = coordinates[1]

    return enrichment


def _load_preview_payload(payload_text: str) -> object:
    normalized = payload_text.strip()
    if normalized.startswith(")]}'"):
        normalized = normalized[4:].lstrip()
    try:
        return json.loads(normalized)
    except json.JSONDecodeError:
        return None


def _clean_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.split()).strip()
    if not normalized:
        return None
    return normalized


def _first_nonempty(lines: list[str]) -> str | None:
    for line in lines:
        if line:
            return line
    return None


def _body_lines(value: object) -> list[str]:
    if not isinstance(value, str):
        return []
    return [line.strip() for line in value.splitlines() if line.strip()]


def _dedupe_lines(lines: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for line in lines:
        if line in seen:
            continue
        deduped.append(line)
        seen.add(line)
    return deduped


def _extract_secondary_name(lines: list[str], *, name: str | None) -> str | None:
    if name is None:
        return None
    try:
        start = lines.index(name)
    except ValueError:
        return None
    for line in lines[start + 1 : start + 4]:
        if line == name:
            continue
        if _parse_rating(line) is not None:
            return None
        if _extract_category_from_lines([line]) is not None:
            return None
        return line
    return None


def _extract_category_from_lines(lines: list[str]) -> str | None:
    for line in lines:
        if "·" not in line:
            continue
        category = line.split("·", 1)[0].strip()
        if category:
            return category
    return None


def _extract_address_from_lines(lines: list[str]) -> str | None:
    for line in lines:
        if _looks_like_address_line(line):
            return line
    return None


def _looks_like_address_line(line: str) -> bool:
    lowered = line.lower()
    if lowered.startswith(("http://", "https://", "www.")):
        return False
    if lowered.startswith(("closed", "open")) or "opens " in lowered:
        return False
    if _PHONE_PATTERN.match(line):
        return False
    if _PLUS_CODE_PATTERN.search(line):
        return False
    if _parse_rating(line) is not None and "★" not in line and "star" not in lowered:
        if re.fullmatch(r"[0-9]+(?:[.,][0-9]+)?", line.strip()):
            return False
    if "〒" in line or line.startswith("Japan, "):
        return True
    if _POSTAL_CODE_PATTERN.search(line) and any(character.isalpha() for character in line):
        return True
    if re.search(r"\d", line) is None:
        return False
    if "," in line and any(character.isalpha() for character in line):
        return True
    return _ADDRESS_KEYWORD_PATTERN.search(line) is not None


def _extract_status_from_lines(lines: list[str]) -> str | None:
    for line in lines:
        lowered = line.lower()
        if lowered.startswith("closed") or lowered.startswith("open") or "opens " in lowered:
            return line
    return None


def _extract_phone_from_lines(lines: list[str]) -> str | None:
    phone_pattern = re.compile(r"^\+?[0-9][0-9()\-\s]{7,}$")
    for line in lines:
        if phone_pattern.match(line):
            return line
    return None


def _extract_plus_code_from_lines(lines: list[str]) -> str | None:
    for line in lines:
        match = _PLUS_CODE_PATTERN.search(line)
        if match is not None:
            return match.group(0).strip()
    return None


def _extract_description(snapshot: Mapping[str, object], lines: list[str]) -> str | None:
    direct = _clean_text(snapshot.get("description"))
    if direct is not None:
        return direct
    for index, line in enumerate(lines):
        if line.startswith("Seasonal ") or line.startswith("Modern setting "):
            return line
        if line == "Share" and index + 1 < len(lines):
            candidate = lines[index + 1]
            if candidate.lower() not in _DESCRIPTION_STOP_MARKERS:
                return candidate
    return None


def _extract_preview_website(strings: list[str]) -> str | None:
    for value in strings:
        for candidate in re.findall(r"https?://[^\s\"'<>]+", value):
            normalized = _normalize_preview_website(candidate)
            if normalized is not None:
                return normalized
    return None


def _normalize_preview_website(value: str) -> str | None:
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"}:
        return None
    if parsed.netloc.endswith("google.com") or parsed.netloc.endswith("gstatic.com"):
        query = parse_qs(parsed.query)
        target = query.get("q", [None])[0]
        if target is None:
            return None
        return _normalize_preview_website(unquote(target))
    if "googleusercontent.com" in parsed.netloc:
        return None
    return value


def _extract_preview_phone(strings: list[str]) -> str | None:
    best_local: str | None = None
    for value in strings:
        if not _PHONE_PATTERN.match(value.strip()):
            continue
        normalized = value.strip()
        if normalized.startswith("+"):
            return normalized
        if best_local is None:
            best_local = normalized
    return best_local


def _extract_preview_plus_code(strings: list[str]) -> str | None:
    compound_match: str | None = None
    for value in strings:
        match = _PLUS_CODE_PATTERN.search(value)
        if match is not None:
            candidate = match.group(0).strip()
            if " " in candidate:
                return candidate
            if compound_match is None:
                compound_match = candidate
    return compound_match


def _extract_preview_address(strings: list[str]) -> str | None:
    candidates: list[str] = []
    for value in strings:
        normalized = value.strip()
        if "maps/preview/place" in normalized or normalized.startswith("/g/"):
            continue
        if "〒" in normalized or normalized.startswith("Japan, "):
            candidates.append(normalized)
            continue
        if normalized.count(",") >= 2 and re.search(r"\d", normalized):
            candidates.append(normalized)
    if not candidates:
        return None
    return max(candidates, key=len)


def _extract_preview_category(root: list[object], strings: list[str]) -> str | None:
    for node in _iter_lists(root):
        if not node or not all(isinstance(value, str) for value in node):
            continue
        text_items = [cast(str, value).strip() for value in node]
        if (
            1 <= len(text_items) <= 4
            and all(_looks_like_category_text(item) for item in text_items)
        ):
            return text_items[0]

    for value in strings:
        if not value.startswith("SearchResult.TYPE_"):
            continue
        category = value.removeprefix("SearchResult.TYPE_").replace("_", " ").strip().lower()
        if category:
            return category.capitalize()
    return None


def _looks_like_category_text(value: str) -> bool:
    if not value or len(value) > 60:
        return False
    if re.search(r"\d", value):
        return False
    if value.startswith(("http://", "https://", "/g/")):
        return False
    if "," in value:
        return False
    return _CATEGORY_SUFFIX_PATTERN.search(value) is not None


def _extract_preview_description(strings: list[str]) -> str | None:
    candidates = [
        value.strip()
        for value in strings
        if len(value.split()) >= 4
        and "SearchResult.TYPE_" not in value
        and "support.google.com" not in value
        and "local/content/rap/report" not in value
        and "〒" not in value
        and not value.startswith("Japan, ")
        and value.count(",") < 2
    ]
    if not candidates:
        return None
    return max(candidates, key=len)


def _extract_preview_coordinates(root: list[object]) -> tuple[float, float] | None:
    fallback_e7_pair: tuple[float, float] | None = None
    for node in _iter_lists(root):
        if len(node) == 4 and node[0] is None and node[1] is None:
            lat = _parse_float(node[2])
            lng = _parse_float(node[3])
            if _valid_coordinates(lat, lng):
                return (cast(float, lat), cast(float, lng))
        if len(node) == 2 and all(isinstance(value, int) for value in node):
            lat_e7 = cast(int, node[0])
            lng_e7 = cast(int, node[1])
            if not _looks_like_e7_coordinate_pair(lat_e7, lng_e7):
                continue
            lat = lat_e7 / 10_000_000
            lng = lng_e7 / 10_000_000
            if _valid_coordinates(lat, lng) and fallback_e7_pair is None:
                fallback_e7_pair = (lat, lng)
    return fallback_e7_pair


def _looks_like_e7_coordinate_pair(lat_e7: int, lng_e7: int) -> bool:
    if lat_e7 == 0 and lng_e7 == 0:
        return True
    return max(abs(lat_e7), abs(lng_e7)) >= 10_000


def _iter_strings(node: object) -> Iterable[str]:
    if isinstance(node, str):
        yield node
        return
    if isinstance(node, list):
        for item in node:
            yield from _iter_strings(item)
        return
    if isinstance(node, dict):
        for item in node.values():
            yield from _iter_strings(item)


def _iter_lists(node: object) -> Iterable[list[object]]:
    if isinstance(node, list):
        yield node
        for item in node:
            yield from _iter_lists(item)
        return
    if isinstance(node, dict):
        for item in node.values():
            yield from _iter_lists(item)


def _is_meaningful_preview_string(value: str) -> bool:
    normalized = value.strip()
    if not normalized or len(normalized) > 400:
        return False
    if (
        normalized.startswith("0ahUKE")
        or normalized.startswith("EvgD")
        or normalized.startswith("UF3g")
    ):
        return False
    return True


def _parse_rating(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None
    match = re.search(r"([0-9]+(?:[.,][0-9]+)?)", value)
    if match is None:
        return None
    try:
        return float(match.group(1).replace(",", "."))
    except ValueError:
        return None


def _parse_review_count(value: object) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if not isinstance(value, str):
        return None
    match = re.search(r"([0-9][0-9,.\s]*)([KM萬万]?)", value.strip(), re.IGNORECASE)
    if match is None:
        return None
    number_text = match.group(1).strip()
    suffix = match.group(2).upper()
    if not suffix and re.fullmatch(r"\d{1,3}(?:[.,\s]\d{3})+", number_text):
        return int(re.sub(r"[.,\s]", "", number_text))
    try:
        number = float(number_text.replace(",", "").replace(" ", ""))
    except ValueError:
        return None
    multiplier = 1
    if suffix == "K":
        multiplier = 1_000
    elif suffix == "M":
        multiplier = 1_000_000
    elif suffix in {"萬", "万"}:
        multiplier = 10_000
    return int(number * multiplier)


def _normalize_website(value: object) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None
    if text.startswith("http://") or text.startswith("https://"):
        return text
    return text


def _extract_coordinate_from_url(url: str, *, index: int) -> float | None:
    match = re.search(r"@(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?)", url)
    if match is None:
        return None
    try:
        return float(match.group(index + 1))
    except ValueError:
        return None


def _to_bool(value: object) -> bool:
    return value is True


def _parse_float(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _valid_coordinates(lat: float | None, lng: float | None) -> bool:
    if lat is None or lng is None:
        return False
    return -90 <= lat <= 90 and -180 <= lng <= 180
