"""Helpers for locating Google Maps placelist data."""

from __future__ import annotations

import re
from urllib.parse import urlencode

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
