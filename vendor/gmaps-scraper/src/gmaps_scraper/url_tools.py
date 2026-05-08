"""Helpers for locating Google Maps placelist data."""

from __future__ import annotations

import re
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

PLACELIST_URL_MARKER = "maps/placelists/list/"
_LIST_ID_PATTERN = re.compile(r"!2s([^!]+)")
_PLACELIST_ID_PATTERN = re.compile(r"maps/placelists/list/([^/?#\"'\\\\]+)")


def extract_list_id(url: str) -> str | None:
    """Extract the placelist identifier from a Google Maps saved-list URL."""
    match = _LIST_ID_PATTERN.search(url)
    if match is None:
        return None
    return match.group(1)


def extract_list_id_from_text(value: str) -> str | None:
    """Extract a placelist identifier from any string containing placelist signals."""
    from_url = extract_list_id(value)
    if from_url is not None:
        return from_url
    match = _PLACELIST_ID_PATTERN.search(value)
    if match is None:
        return None
    return match.group(1)


def has_placelist_marker(value: str) -> bool:
    """Return whether a string contains the placelist URL marker."""
    return PLACELIST_URL_MARKER in value


def build_maps_search_url(
    query: str,
    *,
    place_id: str | None = None,
    hl: str | None = "en",
    gl: str | None = "us",
) -> str:
    """Build a Google Maps search URL for a caller-provided place query."""
    normalized_query = " ".join(query.split())
    if not normalized_query:
        raise ValueError("query is required")
    params: dict[str, str] = {
        "api": "1",
        "query": normalized_query,
    }
    if place_id is not None and place_id.strip():
        params["query_place_id"] = place_id.strip()
    if hl is not None and hl.strip():
        params["hl"] = hl.strip()
    if gl is not None and gl.strip():
        params["gl"] = gl.strip()
    return f"https://www.google.com/maps/search/?{urlencode(params)}"


def localize_maps_url(
    url: str,
    *,
    hl: str | None = "en",
    gl: str | None = "us",
) -> str:
    """Return a Google Maps URL with explicit language and region parameters.

    `scrape_place()` preserves the caller-provided URL. Use this helper at the
    call site when English-readable Maps UI/output is preferred over the URL's
    original locale.
    """
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return url
    host = (parsed.hostname or "").lower()
    if re.fullmatch(r"(?:www\.|maps\.)?google\.[a-z]{2,}(?:\.[a-z]{2,})?", host) is None:
        return url
    if not host.startswith("maps.google.") and not parsed.path.startswith("/maps/"):
        return url
    query_pairs = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key not in {"hl", "gl"}
    ]
    if hl is not None and hl.strip():
        query_pairs.append(("hl", hl.strip()))
    if gl is not None and gl.strip():
        query_pairs.append(("gl", gl.strip()))
    return urlunparse(parsed._replace(query=urlencode(query_pairs)))
