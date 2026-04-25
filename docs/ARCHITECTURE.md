# Architecture

This document covers the implementation and operational details behind Favorite Places. The README is intentionally kept focused on what the project is and how to use it.

## Stack

- Astro for the static site
- TypeScript for frontend data loading and UI behavior
- Python + `uv` for scraping, normalization, enrichment, and generated JSON
- `bun` for frontend package management and scripts

The repo pins Python via [../.python-version](../.python-version) so local `uv` usage and Cloudflare Pages builds resolve the intended runtime instead of Cloudflare's default.

## Site Pack Contract

The reusable app reads user-owned content from a site pack.

Resolution order:

1. `FAVORITE_PLACES_SITE_DIR`, when set
2. `./site`, when present
3. `./site.example`

Expected shape:

```txt
site/
  config.ts
  theme.css
  list_sources.json
  data/raw/
  data/cache/places.sqlite
  public/
  content/templates/
  overrides/
```

Site-owned content:

- `site/config.ts`: site identity, nav, map provider, home copy, guide labels, section visibility, and card display fields
- `site/theme.css`: CSS custom-property overrides and targeted site-specific styling
- `site/list_sources.json`: public Google Maps list URLs or local Google Takeout CSV source definitions
- `site/data/raw/`: raw source snapshots
- `site/data/cache/`: reproducible enrichment cache artifacts
- `site/overrides/`: manual curation for lists and places
- `site/content/templates/`: trusted HTML insertion points
- `site/public/`: copied into static builds and served during local dev

App-owned code:

- `src/pages/`
- `src/components/`
- `src/lib/`
- `src/styles/`
- `scripts/`

## Template Surface

The template API is layered:

1. `site/config.ts` for copy, labels, behavior, and card display toggles
2. CSS custom properties in `site/theme.css`
3. Known HTML insertion points in `site/content/templates/`
4. App-owned components for interactive cards, maps, search, and filtering

Supported template blocks:

```txt
site/content/templates/
  home-aside.html
  home-before-guides.html
  home-after-guides.html
  guide-aside/default.html
  guide-aside/<guide-slug>.html
  guide-before-places/default.html
  guide-before-places/<guide-slug>.html
  guide-after-places/default.html
  guide-after-places/<guide-slug>.html
```

Card markup stays app-owned because search, filtering, maps, and highlighting depend on stable `data-*` attributes. Customize what cards show through config, then customize the look through CSS.

## Source Definitions

`site/list_sources.json` is a list of source definitions. Each source needs a stable `slug`.

- `url` sources infer `type: "google_list_url"` for supported Google Maps links.
- `path` sources infer `type: "google_export_csv"` and require `title`.
- `type` can be included explicitly, but it must match the configured `url` or `path`.
- `title` is optional for Google Maps URL sources and acts as a fallback title.
- Google My Maps URLs such as `https://www.google.com/maps/d/...` are not supported.

Example:

```json
[
  {
    "slug": "tokyo-japan",
    "url": "https://maps.app.goo.gl/your-public-list"
  },
  {
    "slug": "taipei-taiwan",
    "path": "data/imports/taipei-taiwan.csv",
    "title": "Taipei, Taiwan"
  }
]
```

## Data Workflow

Refresh all configured sources:

```bash
bun run sync:sources
```

Force-refresh all configured sources:

```bash
bun run sync:sources:force
```

Refresh one source by slug, source URL, or source path:

```bash
bun run sync:source -- tokyo-japan
bun run sync:source -- https://maps.app.goo.gl/your-public-list
bun run sync:source -- data/imports/taipei-taiwan.csv
```

Build generated data from current raw snapshots:

```bash
bun run build:data
```

Generated outputs:

- `src/data/generated/`: local-only JSON read by Astro
- `public/data/search-index.json`: local-only browser search index

These generated outputs are intentionally gitignored.

## Refresh Profiles

The profile wrapper is the preferred interface for recurring refresh automation:

```bash
bun run refresh:balanced
bun run refresh:backfill
bun run refresh:sweep
```

- `refresh:balanced`: refreshes due raw sources, then runs normal incremental enrichment. Missing places and raw-place changes go first; stale cache entries are refreshed afterward according to their TTLs.
- `refresh:backfill`: refreshes due raw sources, then fills only missing enrichment and missing photos.
- `refresh:sweep`: refreshes due raw sources, then force-refreshes every enrichment entry as a periodic consistency sweep.

Suggested self-hosted schedule:

```cron
17 3 * * * cd /srv/favorite-places && bun run refresh:balanced
43 5 1 * * cd /srv/favorite-places && bun run refresh:sweep -- --refresh-workers 1
```

GitHub Actions schedules use UTC.

## Self-Hosted Refresh Runner

The repo includes `.github/workflows/data-refresh.yml` for refresh automation on a self-hosted Linux runner.

Recommended boundary:

- Do not run Google Maps list scraping or enrichment refreshes on GitHub-hosted runners.
- Keep `.github/workflows/ci.yml` validation-only.
- Keep `.github/workflows/data-refresh.yml` self-hosted only.
- Treat scraper session state, proxy routing, and anti-bot handling as infrastructure concerns.
- If the self-hosted runner is unavailable, skip the refresh rather than falling back to GitHub-hosted infrastructure.

Required repo secrets:

- `GOOGLE_PLACES_API_KEY`, optional for enrichment
- `GMAPS_SCRAPER_PROXY`, optional for scraper traffic
- `GH_AUTOMATION_TOKEN`, optional but recommended for pushing `automation/data-refresh` and opening/updating PRs

Minimum `GH_AUTOMATION_TOKEN` permissions:

- Classic PAT: `repo` and `workflow`
- Fine-grained PAT: repository access to this repo with `Contents: Read and write`, `Pull requests: Read and write`, and `Workflows: Read and write`

Runner provisioning:

- `unzip` must be present on `PATH` for `oven-sh/setup-bun`.
- `cloakbrowser` downloads Chromium, but the host still needs Chromium system libraries.
- Prefer provisioning the full Playwright Chromium dependency set:

```bash
sudo npx playwright install-deps chromium
```

Partial Ubuntu fix for common missing libraries:

```bash
sudo apt-get update
sudo apt-get install -y libnspr4 libnss3 libgbm1 libxcb1 libxkbcommon0
sudo ldconfig
```

Verify:

```bash
ldconfig -p | grep -E 'libnspr4\.so|libnss3\.so|libgbm\.so\.1|libxcb\.so\.1|libxkbcommon\.so\.0'
```

## Cloudflare Pages

Cloudflare Pages should not auto-install Python dependencies for this repo. The root `pyproject.toml` exists for the local data pipeline, and Cloudflare's default `pip` path does not understand the vendored scraper declared in `[tool.uv.sources]`.

Use these Pages settings:

- Environment variable: `SKIP_DEPENDENCY_INSTALL=true`
- Environment variable: `BUN_VERSION=1.3.12`
- Python version: keep [../.python-version](../.python-version) aligned with `pyproject.toml`
- Build command:

```bash
bun ci && pipx install uv==0.11.6 && export PATH="$HOME/.local/bin:$PATH" && uv sync && bun run build:data && bun run build
```

Why:

- `bun ci` installs frontend dependencies from `bun.lock`
- `pipx install uv==0.11.6` makes `uv` available in the Pages build image
- `uv sync` installs Python dependencies, including the vendored scraper from `vendor/gmaps-scraper`
- `bun run build:data` generates Astro input JSON
- `bun run build` builds the static site

## Environment Variables

Common variables:

- `GOOGLE_MAPS_JS_API_KEY`: browser Google Maps display key, read by Astro during render/build and embedded only when the Google map provider is active
- `GOOGLE_PLACES_API_KEY`: server/build-time fallback key for enrichment
- `PUBLIC_MAP_PROVIDER=leaflet`: force Leaflet/OpenStreetMap rendering
- `PUBLIC_PLACE_PHOTOS=off`: hide place photos in the UI
- `FAVORITE_PLACES_SITE_DIR`: point the app and Python pipeline at a non-default site pack
- `GMAPS_SCRAPER_PROXY`: route scraper traffic through a proxy

Use a restricted browser key for `GOOGLE_MAPS_JS_API_KEY`. Do not expose `GOOGLE_PLACES_API_KEY` to the browser.

## Data Model

The project keeps these layers separate:

1. `site/data/raw/`: raw scraper or CSV import snapshots
2. `site/data/cache/places.sqlite`: cached Google Places lookups keyed by guide slug and stable place id
3. `site/data/cache/google-places/`: optional debug export directory, gitignored
4. `site/overrides/`: handwritten metadata, tags, notes, visibility, and ranking
5. `src/data/generated/`: static JSON that Astro reads at build time, gitignored
6. `public/data/search-index.json`: browser search index, gitignored

Merge precedence:

1. Manual overrides
2. Google Places enrichment cache
3. Raw scraped list data

Manual overrides always win over machine-enriched fields.

## Google Places Enrichment

Enrichment is optional and cached. A normal build never calls Google.

- `bun run enrich:data`: normal incremental enrichment; missing places go first, then stale entries
- `bun run fill:enrichment`: only places with no cache entry
- `bun run fill:gaps`: missing enrichment plus missing photos
- `bun run refresh:enrichment`: force-refresh every entry
- `bun run export:cache:json`: optional debug export of per-guide cache JSON from SQLite

The current enrichment pass uses Google Places Text Search with a narrow field mask and location bias around scraped coordinates. It fills useful metadata such as category, Maps URI, rating, business status, and photos without making the site build a runtime dependency on Google.

Cache invalidation is field-aware: raw input changes force refresh, operational places refresh more slowly, and volatile states like ratings, closures, unmatched results, and API errors refresh sooner.

For CSV imports, the pipeline trusts each place's Google Maps URL more than the exported title. It derives stable place IDs from the Maps place token when needed and prefers Google Places display names during normalization so export mojibake does not leak into the final guide.

## Template Repo Workflow

The intended public upstream template repo is [`508-dev/favorite-places`](https://github.com/508-dev/favorite-places).

This repo currently uses a transitional layout where the reusable app is at the repo root and the private site pack lives in `site/`. A later private deployment layout can move the reusable app under `app/` and keep `site/` beside it.

Add the upstream remote:

```bash
git remote add upstream git@github.com:508-dev/favorite-places.git
```

For an `app/ + site/` private deployment repo, point the build at the sibling private site pack:

```bash
cd app
FAVORITE_PLACES_SITE_DIR=../site bun run build:data
FAVORITE_PLACES_SITE_DIR=../site bun run build
```

Pull public app updates into the private deployment repo:

```bash
git subtree pull --prefix app upstream main --squash
```

Contribute reusable app changes back to the public template:

```bash
git subtree push --prefix app upstream my-app-change
```
