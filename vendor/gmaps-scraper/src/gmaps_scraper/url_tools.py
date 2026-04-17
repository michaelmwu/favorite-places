"""Helpers for locating Google Maps placelist data."""

from __future__ import annotations

import re

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
