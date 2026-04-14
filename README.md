# Google Saved List Scraper

Extract places from Google Maps saved lists with either a CLI or a Python import.

The scraper opens the saved-list URL in a real browser session, reads Google Maps
runtime data, and returns structured JSON for the list and its places.

## Requirements

- Python `3.14`
- `uv`
- `cloakbrowser` as a runtime dependency

## Install

This project is intended to be consumed directly from source rather than from PyPI.

### Install From A Local Checkout

Use this when the consumer project and this repo live on the same machine.

```bash
uv add /absolute/path/to/google-saved-list-scraper
```

### Install From Git

Use this when consumers should install directly from a repository.

```bash
uv add git+https://github.com/michaelmwu/google-saved-list-scraper.git
```

### Vendor The Source

Copy `src/google_saved_lists/` into your project and keep its parent directory on the
Python import path. If you vendor the package, also add the runtime dependency:

```bash
uv add cloakbrowser
```

Example vendored layout:

```text
your-project/
  pyproject.toml
  your_app/
  google_saved_lists/
    __init__.py
    cli.py
    models.py
    parser.py
    scraper.py
    url_tools.py
    py.typed
```

## CLI

The package installs a `google-saved-lists` command.

Basic usage:

```bash
uv run google-saved-lists "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18"
```

Write JSON to a file:

```bash
uv run google-saved-lists \
  "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18" \
  --output saved-list.json
```

Run with a visible browser for debugging:

```bash
uv run google-saved-lists \
  "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18" \
  --headed
```

Available CLI options:

- `--output PATH` writes the JSON result to a file
- `--headed` runs the browser in headed mode
- `--timeout-ms INTEGER` controls the navigation timeout
- `--settle-ms INTEGER` adds extra wait time after the page loads

## Library Usage

Import the package directly in application code:

```python
from google_saved_lists import scrape_saved_list

result = scrape_saved_list("https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18")

print(result.list_id)
print(result.resolved_url)
print(result.title)
print(result.to_dict())
```

Public top-level imports intended for consumers:

- `scrape_saved_list`
- `parse_saved_list_artifacts`
- `SavedList`
- `Place`
- `ParseError`
- `ScrapeError`

## Output

The scraper returns a `SavedList` object. `to_dict()` produces JSON like this:

```json
{
  "source_url": "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18",
  "resolved_url": "https://www.google.com/maps/@30.5370705,125.4120472,6z/data=!4m3!11m2!2sUGEPbA20Qd-OH4uoWjmDgQ!3e3?entry=ttu",
  "list_id": "UGEPbA20Qd-OH4uoWjmDgQ",
  "title": "Tokyo Dinners",
  "description": "Best spots in the city",
  "places": [
    {
      "name": "Yakumo",
      "address": "Shibuya, Tokyo",
      "note": "Delicious wonton ramen. You can ask for a mix of white and dark broth.",
      "is_favorite": true,
      "lat": 35.6501307,
      "lng": 139.6868459,
      "maps_url": "https://maps.google.com/?cid=7451636382641713350"
    }
  ]
}
```

`source_url` preserves the caller's input URL. `resolved_url` captures the final browser
URL after redirects, which is useful for short `maps.app.goo.gl` links.

## Behavior Notes

- The scraper is designed for Google Maps saved-list URLs.
- It uses a real browser session because Google Maps does not expose the required data
  reliably to simple HTTP clients.
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
