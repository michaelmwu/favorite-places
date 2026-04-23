# Favorite Places

Static-first personal travel guides built from Google Maps saved lists.

Frontend package management and script execution use `bun`.
The scraper dependency is vendored into this repo as a git subtree at
`vendor/gmaps-scraper/`, and `uv` installs it from that in-repo path.

## Stack

- Astro for the site
- Python + `uv` for scraping, normalization, and enrichment
- Cloudflare Pages or GitHub Pages for static hosting

## Commands

Install frontend dependencies:

```bash
bun install
```

Install Python dependencies:

```bash
uv sync
```

The repo pins Python via [`.python-version`](.python-version) so local `uv` usage and Cloudflare Pages builds both resolve the intended `3.14` runtime instead of Cloudflare's default `3.13.x`.

## Environment Variables

Put local secrets in `.env`.

Recommended split:

```bash
# Browser Google Maps display only.
GOOGLE_MAPS_JS_API_KEY=...

# Optional backup for server/build-time enrichment fallback.
GOOGLE_PLACES_API_KEY=...

# Optional: force the old non-Google map path.
PUBLIC_MAP_PROVIDER=leaflet

# Optional: hide place photos entirely in the UI.
PUBLIC_PLACE_PHOTOS=off
```

Notes:

- `GOOGLE_MAPS_JS_API_KEY` is read by Astro during render/build and embedded into the page only when the Google map provider is active. Treat it as a browser key: restrict the production key by HTTP referrer and allow only `Maps JavaScript API`.
- `GOOGLE_PLACES_API_KEY` should never be exposed to the browser. Use it only as a server/build-time fallback when place-page enrichment cannot recover enough data.
- Use a separate production browser key instead of reusing a local dev key.
- `PUBLIC_MAP_PROVIDER=leaflet` is an escape hatch if you need to force the Leaflet fallback while keeping the Google Maps codepath in the repo.
- `PUBLIC_PLACE_PHOTOS=off` hides place photos in the UI without changing the data pipeline.
- `GMAPS_SCRAPER_PROXY` optionally routes scraper traffic through a proxy. The pipeline keeps proxy-specific scraper sessions under `.context/gmaps-scraper/`, clears them after obvious block/cookie-jar failures, and expires idle sessions after 14 days.

Populate local raw data from public Google Maps lists:

```bash
bun run sync:sources
```

This is the first step in the normal data workflow. It refreshes every configured raw source and rebuilds generated site data.
Public Google Maps URLs are re-scraped only when their `refresh_after` window expires or the source config changes.
Local Google export CSV files are re-imported only when their contents or config change.
Headless refreshes run up to 4 scraper workers in parallel by default. Use
`uv run python3 scripts/build_data.py --refresh --refresh-workers 1` to force
serial execution, or `--headed` to keep browser windows single-worker.

Recommended operator flow:

1. Sync raw sources:

```bash
bun run sync:sources
```

2. Choose one enrichment mode:

```bash
GOOGLE_PLACES_API_KEY=... bun run fill:gaps
```

Use `fill:gaps` for the smallest incremental pass. It only fills totally missing enrichment and downloads any newly available missing photos.

```bash
GOOGLE_PLACES_API_KEY=... bun run refresh:enrichment
```

Use `refresh:enrichment` when you want to sweep and refresh the entire place cache, not just missing entries.

3. Build the site inputs:

```bash
bun run build:data
```

This is also a good fit for automation later:
- `bun run sync:sources`
- then either `bun run fill:gaps` for a light recurring pass or `bun run refresh:enrichment` for a full refresh
- then `bun run build:data`
- then open a PR with the updated raw data / cache snapshot if anything changed

For a recurring refresh job, use the profile wrapper instead of hand-assembling the steps:

```bash
bun run refresh:balanced
```

Profiles:

- `bun run refresh:balanced` runs the balanced incremental pass. It refreshes only due raw sources, then runs normal enrichment. Missing places and raw-place changes go first; stale cache entries are refreshed afterward according to their own TTLs.
- `bun run refresh:backfill` refreshes due raw sources, then fills only missing enrichment and missing photos.
- `bun run refresh:sweep` refreshes due raw sources, then force-refreshes every enrichment entry as a periodic backstop.

Recommended schedule for self-hosted infra:

- Run `bun run refresh:balanced` once per day. This is the default balance: raw snapshots already use a 14-day TTL with stable jitter, and enrichment already prioritizes missing or changed places before TTL-based stale refreshes.
- Run `bun run refresh:sweep -- --refresh-workers 1` about once per month as a slower consistency sweep. That catches long-lived stale fields without paying the cost of force-refreshing every place on every daily job.
- Use `bun run refresh:backfill` or `bun run sync:source -- <slug>` for ad hoc follow-up after adding a new source or when you want a very cheap catch-up pass.

Example cron entries:

```cron
17 3 * * * cd /srv/favorite-places && bun run refresh:balanced
43 5 1 * * cd /srv/favorite-places && bun run refresh:sweep -- --refresh-workers 1
```

GitHub Actions `schedule` uses UTC, so the workflow mirrors those times in UTC rather than the runner's local timezone.

If you prefer GitHub Actions as the control plane, the repo now includes `.github/workflows/data-refresh.yml`.
It is pinned to the self-hosted runner labels `self-hosted` and `Linux`,
runs the refresh job only on that runner class, passes through optional `GOOGLE_PLACES_API_KEY` and
`GMAPS_SCRAPER_PROXY` repo secrets, optionally uses `GH_AUTOMATION_TOKEN` for branch push / PR auth,
and opens or updates a PR from `automation/data-refresh`.

Set `GH_AUTOMATION_TOKEN` as a repo Actions secret if you want the workflow to create or advance
`automation/data-refresh` after commits that touch `.github/workflows/**`.
Minimum token permissions for `GH_AUTOMATION_TOKEN`:

- Classic PAT: `repo` and `workflow`.
- Fine-grained PAT: repository access to this repo with `Contents: Read and write`, `Pull requests: Read and write`, and `Workflows: Read and write`.

Those permissions are enough for the workflow to push the `automation/data-refresh` branch and create or update
the corresponding PR without granting broader access than necessary.

The default `GITHUB_TOKEN` is not sufficient for that case.

Recommended boundary:

- Do not run Google Maps list scraping or enrichment refreshes on GitHub-hosted runners.
- Keep `.github/workflows/ci.yml` validation-only: tests, `bun run build:data` against committed artifacts, and `bun run build`.
- Keep `.github/workflows/data-refresh.yml` self-hosted only. Do not broaden its `runs-on` labels to `ubuntu-latest` or add `push` / `pull_request` triggers.
- Treat scraper session state, proxy routing, and anti-bot handling as infrastructure concerns that belong on the self-hosted runner, not on ephemeral public CI.
- If the self-hosted runner is unavailable, skip the refresh run rather than falling back to GitHub-hosted infrastructure.

For Ubuntu-based self-hosted runners, prefer provisioning the full Playwright Chromium dependency set on the host before using `data-refresh`:

```bash
sudo npx playwright install-deps chromium
```

If preflight is only missing a few common shared libraries on an otherwise-provisioned runner, a partial Ubuntu fix is:

```bash
sudo apt-get update
sudo apt-get install -y libnspr4 libnss3 libgbm1 libxcb1 libxkbcommon0
sudo ldconfig
```

Verify the required libraries are visible to the dynamic linker before rerunning the workflow:

```bash
ldconfig -p | grep -E 'libnspr4\.so|libnss3\.so|libgbm\.so\.1|libxcb\.so\.1|libxkbcommon\.so\.0'
```

Force-refresh raw source imports even if a CSV input is unchanged:

```bash
bun run sync:sources:force
```

Refresh one configured source by slug, source URL, or source path:

```bash
bun run sync:source -- tokyo-japan
bun run sync:source -- https://maps.app.goo.gl/your-public-list
bun run sync:source -- data/imports/taipei-taiwan.csv
```

Build generated site data and the static browser search index from local raw JSON:

```bash
bun run build:data
```

Use this when `data/raw/` is already up to date and you only want to regenerate site inputs and search data.
Configured local CSV sources are auto-imported before rebuild. Public Google Maps URL sources are not refreshed here.

Fill missing or stale place enrichment cache entries, then rebuild:

```bash
GOOGLE_PLACES_API_KEY=... bun run enrich:data
```

This is the default incremental mode. It still refreshes stale entries, but it prioritizes totally missing places first.

Fill only missing enrichment entries:

```bash
GOOGLE_PLACES_API_KEY=... bun run fill:enrichment
```

This skips expiry-based refreshes and only backfills places with no cache entry at all.

Fill only missing enrichment entries, then download any newly available missing photos:

```bash
GOOGLE_PLACES_API_KEY=... bun run fill:gaps
```

Use this for the smallest incremental pass when you want to backfill gaps without touching expired cache entries.

Force-refresh all place enrichment cache entries:

```bash
GOOGLE_PLACES_API_KEY=... bun run refresh:enrichment
```

The same key can live in `.env` as `GOOGLE_PLACES_API_KEY=...`.

Optional debug export of per-guide cache JSON from SQLite:

```bash
bun run export:cache:json
```

Start the site:

```bash
bun run dev
```

Guide pages use Google Maps by default when `GOOGLE_MAPS_JS_API_KEY` is set at build/render time. If it is missing, or if `PUBLIC_MAP_PROVIDER=leaflet`, the site falls back to Leaflet/OpenStreetMap.

Verify the site:

```bash
bun run test
bun run check
bun run build
```

## Cloudflare Pages

Cloudflare Pages should not auto-install Python dependencies for this repo.
The root `pyproject.toml` exists for the local data pipeline, and Cloudflare's
default `pip` install path does not understand the vendored scraper declared in
`[tool.uv.sources]`.

Use these Pages settings instead:

- Environment variable: `SKIP_DEPENDENCY_INSTALL=true`
- Environment variable: `BUN_VERSION=1.3.12`
- Python version: keep the root [`.python-version`](.python-version) in sync with `pyproject.toml`
- Build command:

```bash
bun ci && pipx install uv==0.11.6 && export PATH="$HOME/.local/bin:$PATH" && uv sync && bun run build:data && bun run build
```

Why this is necessary:

- `bun ci` installs the frontend dependencies from `bun.lock`
- `pipx install uv==0.11.6` makes `uv` available in the Pages build image
- `uv sync` installs Python dependencies, including the vendored scraper from `vendor/gmaps-scraper`
- `bun run build:data` generates `src/data/generated/`, which Astro reads at build time
- `bun run build` builds the static site

Do not rely on Cloudflare's automatic Python dependency detection for this repo
unless the packaging layout changes.

## Populate Base Data

This repo can commit raw scraped list snapshots in `data/raw/` and a reproducible SQLite-backed Google Places
enrichment cache in `data/cache/places.sqlite` when you want stable source data in git.
It still does not commit generated build data.

1. Export your saved lists from Google Takeout.

Go to [Google Takeout](https://takeout.google.com/), select `Saved`, and download the export.

![Google Takeout Saved export](docs/images/google-takeout.png)

After extracting the archive, you should get a folder with one or more `.csv` files for your saved lists.

![Google Takeout contents](docs/images/takeout-contents.png)

You can then either keep those CSVs as your own reference data, or use the place names and URLs while
building `scripts/config/list_sources.json`.

2. Add your source definitions to `scripts/config/list_sources.json`.

If you are starting from this repo as a base template, copy the example file first:

```bash
cp scripts/config/list_sources.example.json scripts/config/list_sources.json
```

Every source needs a `slug`.
- `url` sources infer `type: "google_list_url"` for supported Google Maps links, including `https://maps.app.goo.gl/...` shortlinks and `https://www.google.com/maps/...` share links.
- `path` sources infer `type: "google_export_csv"` and require `title`.
- `type` can still be included explicitly, but it must match the configured `url` or `path`.
- `title` is optional for Google Maps URL sources and acts as a fallback list title.
- Google My Maps URLs such as `https://www.google.com/maps/d/...` are not supported yet.

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
    "title": "Taipei, Taiwan 🇹🇼"
  }
]
```

Optional fallback title example:

```json
[
  {
    "slug": "tokyo-japan",
    "url": "https://maps.app.goo.gl/your-public-list",
    "title": "Tokyo, Japan 🇯🇵"
  }
]
```

3. Pull raw list data through the installed scraper dependency:

```bash
bun run sync:sources
```

This writes local JSON files into `data/raw/`, including refresh metadata like `fetched_at`,
`refresh_after`, and a source signature. URL-backed sources skip network refreshes until
their refresh window expires unless the source config changes. CSV-backed sources skip rewrites
when the input file hash is unchanged.
It also rebuilds the generated site JSON afterward.

4. Add manual curation files in `src/data/overrides/`.

Example files live alongside the real override directories:

- `src/data/overrides/lists/list.example.json`
- `src/data/overrides/places/list.example.json`

Per-list example at `src/data/overrides/lists/tokyo-japan.json`:

```json
{
  "city_name": "Tokyo",
  "country_name": "Japan",
  "country_code": "JP",
  "list_tags": ["tokyo", "japan", "food", "coffee"]
}
```

Per-place example at `src/data/overrides/places/tokyo-japan.json`:

```json
{
  "cid:6924437575605096209": {
    "top_pick": true,
    "tags": ["coffee", "nakameguro"],
    "why_recommended": "A very easy first stop."
  }
}
```

5. Optionally fill Google Places enrichment cache:

```bash
bun run enrich:data
```

This updates the SQLite cache at `data/cache/places.sqlite`.

Use:
- `bun run fill:enrichment` to backfill only missing place enrichment
- `bun run fill:gaps` to backfill only missing enrichment plus missing photos
- `bun run enrich:data` for the normal incremental path; missing places go first, then stale entries
- `bun run refresh:enrichment` to sweep and refetch everything

For a simple operator workflow, use:
1. `bun run sync:sources`
2. either `bun run fill:gaps` or `bun run refresh:enrichment`
3. `bun run build:data`

If you want per-guide JSON debug dumps, run `bun run export:cache:json`.

6. Build generated site data:

```bash
bun run build:data
```

This writes local generated JSON into `src/data/generated/` and the client-side search index into `public/data/search-index.json` from the current contents of `data/raw/`.
Configured local CSV sources are imported into `data/raw/<slug>.json` first when needed.

7. Run the site:

```bash
bun run dev
```

If you already have raw JSON from elsewhere, you can skip source refresh and place compatible files directly in `data/raw/<slug>.json`, then run `bun run build:data`.
For a targeted refresh, run `bun run sync:source -- <slug-or-url-or-path>`.
For a full forced refresh, run `bun run sync:sources:force`.

Legacy aliases still work:
- `bun run refresh:data`
- `bun run refresh:data:force`
- `bun run refresh:data:list -- <slug-or-url>`

## Template-Ready Files

This repo can keep personal data and still act as the basis for a cleaner template extraction later.
The key is to keep "replace me" files obvious and colocated with the real paths future users will edit.

- `scripts/config/list_sources.json` is your real source list config.
- `scripts/config/list_sources.example.json` is the starter file for template users.
- `src/data/site.ts` is the site-level branding, favicon, and copy config for this instance.
- `src/data/site.example.ts` shows the expected shape for a new instance.
- `src/data/overrides/lists/*.json` and `src/data/overrides/places/*.json` are real handwritten curation files, excluding `*.example.json`.
- `src/data/overrides/lists/list.example.json` and `src/data/overrides/places/list.example.json` are starter examples showing the expected override shapes.

For future extraction into a dedicated template repo, the split is:

- Engine: `scripts/`, `src/lib/`, `src/components/`, and Astro wiring.
- Content: `scripts/config/list_sources.json`, `data/raw/`, and `src/data/overrides/`.
- Theme and branding: `src/data/site.ts` plus any styling and assets under `src/styles/` and `public/`.

## Data Model

The project keeps three layers separate:

1. `data/raw/` stores disposable scraper output.
2. `data/cache/places.sqlite` stores cached Google Places lookups keyed by guide slug and stable place id.
3. `data/cache/google-places/` is an optional debug export directory and is gitignored.
4. `src/data/overrides/` stores handwritten metadata, tags, notes, and ranking.
5. `src/data/generated/` stores the static JSON that Astro reads at build time.

Manual overrides always win over machine-enriched fields.

## Google Places Enrichment

Enrichment is optional and cached. A normal build never calls Google.

- `--enrich` fills missing or stale cache entries according to the cache entry's own refresh window.
- `--refresh-enrichment` ignores the 30-day cache window and refetches every place.
- Manual overrides still win over Google data.
- Cache invalidation is field-aware: raw input changes force a refresh, operational places refresh more slowly,
  and volatile or risky states like ratings, closures, unmatched results, and API errors refresh sooner.

The current enrichment pass uses Google Places Text Search with a narrow field mask and
location bias around the scraped coordinates. It is meant to fill in useful metadata
such as category, Maps URI, and business status without turning the site build into a
runtime dependency on Google.

For CSV imports, the pipeline trusts each place's Google Maps URL more than the exported title.
It derives stable place IDs from the Maps place token when needed and prefers Google Places display
names during normalization so mojibake in the export does not leak into the final guide.
