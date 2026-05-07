"""Compact reusable selector recipes for Google Maps surfaces."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

DEFAULT_PLACE_SELECTOR_RECIPE: dict[str, Any] = {
    "version": 1,
    "maps_ui_signature": {
        "host": "www.google.com",
        "surface": "place_panel",
        "locale": "en-US",
        "has_classes": ["DUwDvf", "F7nice", "Io6YTe", "jftiEf", "MyEned"],
    },
    "interaction_steps": [
        {
            "name": "open_reviews_tab",
            "selector": "div[role='tablist'] button, button[role='tab']",
            "match_text": "(review|reviews|評論|クチコミ)",
        },
        {
            "name": "expand_review_topics",
            "selector": "button, div[role='button']",
            "match_text": "^\\+\\d+$",
            "reject_aria_label": "photo",
        },
    ],
    "selectors": {
        "name": ["h1.DUwDvf", "h1.lfPIob", "div[role='main'] h1"],
        "rating": [
            "div.F7nice > span > span[aria-hidden='true']:first-child",
            "span.ceNzKf[role='img']",
            "span[role='img'][aria-label*='star']",
        ],
        "review_count": [
            "div.F7nice span[role='img'][aria-label*='reviews' i]",
            "span[role='img'][aria-label*='reviews' i]",
            "div.F7nice",
        ],
        "category": [
            "button[jsaction*='category']",
            ".skqShb .fontBodyMedium button",
            "button.DkEaL",
        ],
        "address": [
            "[data-item-id='address'] .Io6YTe",
            "[data-item-id='address']",
            "button[aria-label*='address' i]",
            "div[aria-label*='address' i]",
        ],
        "website": ["a[data-item-id='authority']", "[data-item-id='authority']"],
        "phone": ["button[data-item-id^='phone:'] .Io6YTe", "button[data-item-id^='phone:']"],
        "plus_code": ["[data-item-id='oloc'] .Io6YTe", "[data-item-id='oloc']"],
        "review_topic_chip": [
            "button[aria-label*='mentioned in'][aria-label*='reviews']",
            "button[role='radio']",
            "button[aria-pressed]",
        ],
        "review_root": ["[data-review-id]", ".jftiEf"],
        "review_text": [".MyEned .wiI7pd", ".wiI7pd", ".MyEned"],
        "review_author": [".d4r55", ".WNxzHc"],
        "review_rating": ["[role='img'][aria-label*='star' i]"],
        "review_time": [".rsqaWe", ".xRkPPb"],
    },
    "parser_patterns": {
        "review_topic_aria_label": "{label}, mentioned in {count} reviews",
        "review_count_line": "{count} reviews",
        "review_count_header": "({count})",
    },
    "notes": {
        "address": (
            "If data-item-id address selectors fail, find the row whose icon or "
            "aria-label identifies an address and read its adjacent text."
        )
    },
    "validation": {
        "required_fields": ["name", "address", "rating", "review_count"],
        "validated_examples": [],
    },
}


def default_place_selector_recipe() -> dict[str, Any]:
    """Return a copy of the built-in Google Maps place selector recipe."""
    return deepcopy(DEFAULT_PLACE_SELECTOR_RECIPE)


def load_place_selector_recipe(path: Path) -> dict[str, Any]:
    """Load a compact place selector recipe JSON file."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Place selector recipe must be a JSON object.")
    version = payload.get("version")
    if not isinstance(version, int):
        raise ValueError("Place selector recipe must include an integer version.")
    selectors = payload.get("selectors")
    if not isinstance(selectors, dict):
        raise ValueError("Place selector recipe must include selector mappings.")
    return payload


def write_default_place_selector_recipe(path: Path) -> Path:
    """Write the built-in place selector recipe as compact reusable JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(default_place_selector_recipe(), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return path
