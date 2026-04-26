# GMaps Scraper

Extract data from Google Maps saved lists and individual place pages.

The scraper fetches Google Maps URLs, reads runtime data or the rendered place
panel, and returns structured JSON.

## Requirements

- Python `3.14`
- `uv`
- `curl_cffi` for the primary fetch path
- `cloakbrowser` for browser mode and HTTP fallback

## Install

This project is intended to be consumed directly from source rather than from PyPI.

```bash
uv add git+https://github.com/michaelmwu/gmaps-scraper.git
```

If you vendor the package, also add the runtime dependency:

```bash
uv add curl-cffi cloakbrowser
```

## CLI

The package installs a `gmaps-scraper` command.

Basic usage:

```bash
uv run gmaps-scraper "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18"
```

Scrape a place page:

```bash
uv run gmaps-scraper \
  "https://www.google.com/maps/place/Den/@35.6731762,139.7127216,17z" \
  --kind place
```

Scrape a place page and download its representative image locally:

```bash
uv run gmaps-scraper \
  "https://www.google.com/maps/place/Den/@35.6731762,139.7127216,17z" \
  --kind place \
  --download-photo den-photo.jpg
```

Scrape a place page and download the main place photo specifically:

```bash
uv run gmaps-scraper \
  "https://www.google.com/maps/place/Den/@35.6731762,139.7127216,17z" \
  --kind place \
  --download-main-photo den-main-photo.jpg
```

Explicit fetch modes:

```bash
uv run gmaps-scraper URL --fetch-mode auto
uv run gmaps-scraper URL --fetch-mode curl
uv run gmaps-scraper URL --fetch-mode browser
```

Mode behavior:

- `auto` uses `curl_cffi` first and falls back to the browser if parsing fails
- `curl` uses only the HTTP path
- `browser` uses only the browser path

Write JSON to a file:

```bash
uv run gmaps-scraper \
  "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18" \
  --output saved-list.json
```

Run with a visible browser for debugging:

```bash
uv run gmaps-scraper \
  "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18" \
  --headed
```

Available CLI options:

- `--kind {list,place}` selects which scraper to run
- `--output PATH` writes the JSON result to a file
- `--download-photo PATH` saves the place photo to a local file when scraping a place
- `--download-main-photo PATH` saves the main place photo to a local file when available
- `--headed` runs the browser in headed mode
- `--fetch-mode {auto,curl,browser}` selects the transport path
- `--session-dir PATH` reuses a persistent browser profile for browser fetches
- `--http-cookie-jar PATH` persists curl cookies across fetches
- `--proxy URL` sends curl and browser traffic through a proxy
- `--timeout-ms INTEGER` controls the overall fetch timeout
- `--settle-ms INTEGER` adds extra browser-only wait time after the page loads

## Library Usage

Import the package directly in application code:

```python
from pathlib import Path

from gmaps_scraper import (
    BrowserSessionConfig,
    HttpSessionConfig,
    scrape_place,
    scrape_saved_list,
)

result = scrape_saved_list(
    "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
    browser_session=BrowserSessionConfig(
        profile_dir=Path(".gmaps-scraper/session"),
    ),
    http_session=HttpSessionConfig(
        cookie_jar_path=Path(".gmaps-scraper/http-cookies.txt"),
    ),
)
place = scrape_place("https://www.google.com/maps/place/Den/@35.6731762,139.7127216,17z")

print(result.list_id)
print(result.resolved_url)
print(result.title)
print(result.to_dict())
print(place.review_count)
```

Public top-level imports intended for consumers:

- `BrowserProxyConfig`
- `BrowserSessionConfig`
- `HttpSessionConfig`
- `scrape_saved_list`
- `scrape_place`
- `parse_saved_list_artifacts`
- `ListOwner`
- `SavedList`
- `Place`
- `PlaceDetails`
- `ParseError`
- `ScrapeError`

## Output

A saved list result looks like this:

```json
{
  "source_url": "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
  "resolved_url": "https://www.google.com/maps/@30.5370705,125.4120472,6z/data=!4m3!11m2!2sUGEPbA20Qd-OH4uoWjmDgQ!3e3?entry=ttu",
  "list_id": "UGEPbA20Qd-OH4uoWjmDgQ",
  "title": "Tokyo Dinners",
  "description": "Best spots in the city",
  "owner": {
    "name": "Michael Wu",
    "photo_url": "https://lh3.googleusercontent.com/a-/ALV-UjW_i8-Eyr6conUhZ6tzGGlFe76mQTGeURI9NKDlca0FzlN0GY0Kjg",
    "profile_id": "104356373423434804635"
  },
  "collaborators": [
    {
      "name": "Micca Guan",
      "photo_url": "https://lh3.googleusercontent.com/a-/ALV-UjW_collaborator",
      "profile_id": "107609938540508038600"
    }
  ],
  "places": [
    {
      "name": "Yakumo",
      "address": "Shibuya, Tokyo",
      "note": "Delicious wonton ramen. You can ask for a mix of white and dark broth.",
      "is_favorite": true,
      "lat": 35.6501307,
      "lng": 139.6868459,
      "maps_url": "https://www.google.com/maps/search/?api=1&query=Yakumo%2C+Shibuya%2C+Tokyo",
      "added_by": {
        "name": "Micca Guan",
        "profile_id": "107609938540508038600"
      }
    }
  ]
}
```

`source_url` preserves the caller's input URL. `resolved_url` captures the final URL
after redirects, which is useful for short `maps.app.goo.gl` links.

For place pages, the scraper returns a `PlaceDetails` object with fields such as
`name`, `category`, `rating`, `review_count`, `address`, `status`, `website`,
`phone`, `plus_code`, `main_photo_url`, `photo_url`, and coordinates when available.

## Behavior Notes

- Saved lists default to `curl_cffi` against Google Maps' preloaded XSSI endpoints.
- `--settle-ms` only affects browser fetches. `--timeout-ms` applies to both browser and curl.
- Reuse `HttpSessionConfig(cookie_jar_path=...)` or `--http-cookie-jar` when you want curl
  fetches to carry cookies across runs.
- Place pages currently use the browser path and extract review metadata from the
  rendered DOM.
- `main_photo_url` is the direct main place photo when the rendered DOM exposes one.
- `photo_url` remains the best available image and falls back to the page's
  representative Maps image when the main photo isn't available.
- Browser automation remains available for debugging, consent flows, and fallback.
- By default each scrape uses a fresh browser session. Reuse a profile directory only
  when you want cookies, localStorage, and other browser state to persist across runs.
- Session rotation, clearing blocked profiles, and coordinating proxies across many
  scraping identities are caller-level policy decisions. The library only exposes the
  browser profile and proxy primitives needed to implement that policy.
- Parsing is defensive and tolerates partial metadata, but Google can change its runtime
  schema at any time.
- The parser prefers the explicit placelist ID from the resolved URL when available.

## Development

Install the dev environment:

```bash
uv sync --dev
```

Run the quality gates:

```bash
./scripts/lint.sh
./scripts/typecheck.sh
```

Run tests:

```bash
uv run python -m unittest discover -s tests
```
