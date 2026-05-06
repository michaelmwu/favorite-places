# Repository Guide

## Scope

This repo is for a Python scraper that extracts Google Maps saved lists and
individual Google Maps place pages.

## Tooling

- Use `uv` for Python commands and dependency management.
- Prefer `uv run python ...` over raw `python3`.
- Keep the implementation in Python.
- Target Python `3.14`.
- The latest stable patch release is `3.14.3` as of March 12, 2026, but the repo pins the `3.14` series so `uv` can use the newest available stable patch on each platform.
- Local quality gates are `./scripts/lint.sh` and `./scripts/typecheck.sh`.

## Saved List Workflow

1. Resolve the saved-list URL.
2. Prefer the HTTP/preloaded payload path when available.
3. Fall back to browser artifacts when needed.
4. Read `APP_INITIALIZATION_STATE`, preloaded XSSI payloads, or equivalent runtime data.
5. Locate the placelist payload.
6. Parse list metadata, owner/collaborators, and place entries into structured output.

## Place Page Workflow

1. Load the place URL in a real browser environment.
2. Extract structured DOM rows from the rendered place panel.
3. Open Reviews and About tabs when enabled to collect review topics, visible
   review snippets, and About attributes.
4. Use preview payloads and conservative text fallbacks for missing facts.
5. Build diagnostics and quality flags.
6. Optionally run caller-provided LLM repair tasks only when enabled by policy.

## Parsing Rules

- Treat the explicit placelist ID as the strongest signal.
- First try to extract the list ID from the URL `!2s...` segment.
- When scanning runtime strings, prefer candidates that contain the exact list ID.
- If no exact list ID match is found, fall back to strings containing `maps/placelists/list/`.
- Treat the placelist URL marker as a locator, not as proof that the surrounding node is the correct final parse target.
- Prefer resilient structural detection over hardcoded deep indexes.

## Place Detection

- Detect place records by the coordinate tuple pattern `[null, null, lat, lng]`.
- Use the surrounding parent structure to recover the place name, address, and Google Maps identifier.
- Expect the schema to drift; keep extraction defensive and tolerate missing fields.

## Saved List Output Contract

Saved-list scraping returns structured JSON shaped like:

```json
{
  "source_url": "https://maps.app.goo.gl/...",
  "resolved_url": "https://www.google.com/maps/@.../data=!4m3!11m2!2sLIST_ID!3e3",
  "list_id": "UGEPbA20Qd-OH4uoWjmDgQ",
  "title": "string",
  "description": "string",
  "owner": {
    "name": "string",
    "photo_url": "https://...",
    "profile_id": "string"
  },
  "collaborators": [
    {
      "name": "string",
      "photo_url": "https://...",
      "profile_id": "string"
    }
  ],
  "places": [
    {
      "name": "string",
      "address": "string",
      "note": "string",
      "is_favorite": false,
      "lat": 0.0,
      "lng": 0.0,
      "maps_url": "https://www.google.com/maps/search/?api=1&query=...",
      "cid": "string",
      "google_id": "string",
      "added_by": {
        "name": "string",
        "profile_id": "string"
      }
    }
  ]
}
```

Optional fields are omitted or set to `null` depending on the model's
serialization behavior. Keep output backward-compatible for downstream JSON
consumers.

## Place Output Contract

Place scraping returns `PlaceDetails` JSON shaped like:

```json
{
  "source_url": "https://www.google.com/maps/search/?api=1&query=...",
  "resolved_url": "https://www.google.com/maps/place/...",
  "google_place_id": "ChIJ...",
  "name": "string",
  "secondary_name": "string",
  "category": "string",
  "category_display_en": "string",
  "category_display_en_source": "translation_memory",
  "category_display_en_confidence": "high",
  "rating": 4.8,
  "review_count": 832,
  "price_range": "SGD 100+",
  "address": "raw Google address",
  "address_display_en": "English-readable address",
  "address_display_en_source": "llm",
  "address_display_en_confidence": "high",
  "located_in": "string",
  "status": "Open ⋅ Closes 10 PM",
  "website": "https://example.com",
  "phone": "+1 555-555-5555",
  "plus_code": "string",
  "address_parts": ["structured", "parts"],
  "description": "string",
  "main_photo_url": "https://...",
  "photo_url": "https://...",
  "lat": 0.0,
  "lng": 0.0,
  "limited_view": false,
  "review_topics": [
    {
      "label": "pho",
      "count": 24
    }
  ],
  "reviews": [
    {
      "author": "string",
      "rating": 5.0,
      "relative_time": "2 months ago",
      "text": "visible review snippet",
      "like_count": 1
    }
  ],
  "about_sections": [
    {
      "title": "Accessibility",
      "items": [
        {
          "label": "Wheelchair accessible entrance",
          "aria_label": "Has wheelchair accessible entrance"
        }
      ]
    }
  ],
  "diagnostics": {
    "quality_flags": [],
    "llm_used": false,
    "repair_source": "cache",
    "confidence": 1.0,
    "evidence_hash": "string",
    "prompt_version": "string"
  }
}
```

Preserve raw Google fields. Put English-readable display values in separate
`*_display_en` fields rather than overwriting raw `address` or `category`.
LLM repair is task-scoped: `dom_repair` repairs generic Google Maps facts, and
`display_translation` produces English-readable address/category display fields.
Reviews and About panel collection are enabled by default for full place output,
but callers may skip those extra tab interactions when refreshing overview-only
facts.
Downstream product concepts such as tags, neighborhoods, guide keywords, and
ranking logic do not belong in this package.

## Validation

- Add fixtures for saved-list and place payloads when available.
- Test both primary and fallback paths.
- Verify that parsing still works when optional metadata is missing.
- Run `./scripts/lint.sh`, `./scripts/typecheck.sh`, and focused unit tests for
  changed behavior.
