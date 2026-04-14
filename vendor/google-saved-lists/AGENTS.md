# Repository Guide

## Scope

This repo is for a Python scraper that extracts places from Google Maps saved lists.

## Tooling

- Use `uv` for Python commands and dependency management.
- Prefer `uv run python ...` over raw `python3`.
- Keep the implementation in Python.
- Target Python `3.14`.
- The latest stable patch release is `3.14.3` as of March 12, 2026, but the repo pins the `3.14` series so `uv` can use the newest available stable patch on each platform.
- Local quality gates are `./scripts/lint.sh` and `./scripts/typecheck.sh`.

## Workflow

1. Load the saved list URL in a real browser environment.
2. Read `APP_INITIALIZATION_STATE` or equivalent runtime data.
3. Locate the placelist payload.
4. Parse list metadata and place entries into structured output.

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

## Output Contract

Return structured JSON shaped like:

```json
{
  "source_url": "https://www.google.com/maps/@.../data=!4m3!11m2!2sLIST_ID!3e3",
  "list_id": "UGEPbA20Qd-OH4uoWjmDgQ",
  "title": "string",
  "description": "string",
  "places": [
    {
      "name": "string",
      "address": "string",
      "lat": 0.0,
      "lng": 0.0,
      "maps_url": "https://maps.google.com/?cid=..."
    }
  ]
}
```

## Validation

- Add fixtures for saved-list payloads when available.
- Test both the primary path and the fallback path.
- Verify that parsing still works when optional metadata is missing.
