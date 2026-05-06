"""Helpers for reusing English-readable display fields across refreshes."""

from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import replace
from typing import cast

from gmaps_scraper.models import PlaceDetails
from gmaps_scraper.translation_memory import needs_display_en

_DISPLAY_FIELD_GROUPS = (
    (
        "category",
        "category_display_en",
        "category_display_en_source",
        "category_display_en_confidence",
    ),
    (
        "address",
        "address_display_en",
        "address_display_en_source",
        "address_display_en_confidence",
    ),
)


def reusable_place_display_fields(
    current_fields: Mapping[str, object],
    previous_fields: Mapping[str, object],
    *,
    accepted_sources: Collection[str] | None = None,
    accepted_confidences: Collection[str] | None = None,
) -> dict[str, object]:
    """Return prior display fields that are safe to reuse for refreshed place data."""
    reusable: dict[str, object] = {}
    for raw_key, display_key, source_key, confidence_key in _DISPLAY_FIELD_GROUPS:
        current_raw = _clean_display_value(current_fields.get(raw_key))
        previous_raw = _clean_display_value(previous_fields.get(raw_key))
        if current_raw is None or current_raw != previous_raw:
            continue

        previous_display = _clean_display_value(previous_fields.get(display_key))
        if previous_display is None or needs_display_en(previous_display):
            continue

        current_display = _clean_display_value(current_fields.get(display_key))
        if current_display is not None and not needs_display_en(current_display):
            continue

        previous_source = _clean_display_value(previous_fields.get(source_key))
        if accepted_sources is not None and previous_source not in accepted_sources:
            continue

        previous_confidence = _clean_display_value(previous_fields.get(confidence_key))
        if (
            accepted_confidences is not None
            and previous_confidence not in accepted_confidences
        ):
            continue

        reusable[display_key] = previous_display
        if previous_source is not None:
            reusable[source_key] = previous_source
        if previous_confidence is not None:
            reusable[confidence_key] = previous_confidence
    return reusable


def reuse_place_display_fields(
    current: PlaceDetails,
    previous: PlaceDetails,
    *,
    accepted_sources: Collection[str] | None = None,
    accepted_confidences: Collection[str] | None = None,
) -> PlaceDetails:
    """Return a copy of ``current`` with reusable prior display fields applied."""
    reusable = reusable_place_display_fields(
        _place_display_values(current),
        _place_display_values(previous),
        accepted_sources=accepted_sources,
        accepted_confidences=accepted_confidences,
    )
    if not reusable:
        return current
    return replace(
        current,
        category_display_en=cast(
            str | None,
            reusable.get("category_display_en", current.category_display_en),
        ),
        category_display_en_source=cast(
            str | None,
            reusable.get(
                "category_display_en_source",
                current.category_display_en_source,
            ),
        ),
        category_display_en_confidence=cast(
            str | None,
            reusable.get(
                "category_display_en_confidence",
                current.category_display_en_confidence,
            ),
        ),
        address_display_en=cast(
            str | None,
            reusable.get("address_display_en", current.address_display_en),
        ),
        address_display_en_source=cast(
            str | None,
            reusable.get("address_display_en_source", current.address_display_en_source),
        ),
        address_display_en_confidence=cast(
            str | None,
            reusable.get(
                "address_display_en_confidence",
                current.address_display_en_confidence,
            ),
        ),
    )


def _place_display_values(place: PlaceDetails) -> dict[str, object]:
    return {
        "category": place.category,
        "category_display_en": place.category_display_en,
        "category_display_en_source": place.category_display_en_source,
        "category_display_en_confidence": place.category_display_en_confidence,
        "address": place.address,
        "address_display_en": place.address_display_en,
        "address_display_en_source": place.address_display_en_source,
        "address_display_en_confidence": place.address_display_en_confidence,
    }


def _clean_display_value(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.split())
    return normalized or None
