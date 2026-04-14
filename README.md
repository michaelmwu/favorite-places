# Favorite Places

Static-first personal travel guides built from Google Maps saved lists.

Frontend package management and script execution use `pnpm`.
The scraper dependency is installed via `uv` from the public
`michaelmwu/google-saved-list-scraper` GitHub repo, so worktrees do not need a sibling checkout.

## Stack

- Astro for the site
- Python + `uv` for scraping, normalization, and enrichment
- Cloudflare Pages or GitHub Pages for static hosting

## Commands

Install frontend dependencies:

```bash
pnpm install
```

Install Python dependencies:

```bash
uv sync
```

Optional local Google Places API key:

```bash
cp .env.example .env
```

Populate local raw data from public Google Maps lists:

```bash
pnpm run sync:sources
```

This refreshes every configured source and then rebuilds generated site data.
Public Google Maps URLs are always re-scraped. Local Google export CSV files are re-imported only when their
contents or config change.

Force-refresh raw source imports even if a CSV input is unchanged:

```bash
pnpm run sync:sources:force
```

Refresh one configured source by slug, source URL, or source path:

```bash
pnpm run sync:source -- tokyo-japan
pnpm run sync:source -- https://maps.app.goo.gl/your-public-list
pnpm run sync:source -- data/imports/taipei-taiwan.csv
```

Build generated site data from local raw JSON:

```bash
pnpm run build:data
```

Use this when `data/raw/` is already up to date and you only want to regenerate site inputs.
For configured CSV sources, this expects the matching `data/raw/<slug>.json` to already exist.
If it does not, run `pnpm run refresh:data` or `pnpm run refresh:data:list -- <slug>` first.

Fill missing or stale Google Places enrichment cache entries, then rebuild:

```bash
GOOGLE_PLACES_API_KEY=... pnpm run enrich:data
```

The same key can live in `.env` as `GOOGLE_PLACES_API_KEY=...`.

Force-refresh all Google Places enrichment cache entries:

```bash
GOOGLE_PLACES_API_KEY=... pnpm run refresh:enrichment
```

Start the site:

```bash
pnpm run dev
```

Verify the site:

```bash
pnpm run check
pnpm run build
```

## Populate Base Data

This repo can commit raw scraped list snapshots in `data/raw/` and reproducible Google Places
enrichment cache files in `data/cache/google-places/` when you want stable source data in git.
It still does not commit generated build data.

1. Add your source definitions to `scripts/config/list_sources.json`.

Every source needs a `slug` and a `type`.
- `google_list_url` sources require `url`
- `google_export_csv` sources require `path` and `title`
- `title` is optional only for `google_list_url` and acts as a fallback list title

Example:

```json
[
  {
    "slug": "tokyo-japan",
    "type": "google_list_url",
    "url": "https://maps.app.goo.gl/your-public-list"
  },
  {
    "slug": "taipei-taiwan",
    "type": "google_export_csv",
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
    "type": "google_list_url",
    "url": "https://maps.app.goo.gl/your-public-list",
    "title": "Tokyo, Japan 🇯🇵"
  }
]
```

2. Pull raw list data through the installed scraper dependency:

```bash
pnpm run sync:sources
```

This writes local JSON files into `data/raw/`, including refresh metadata like `fetched_at`
and a source signature. CSV-backed sources skip rewrites when the input file hash is unchanged.
It also rebuilds the generated site JSON afterward.

3. Add manual curation files in `src/data/overrides/`.

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

4. Optionally fill Google Places enrichment cache:

```bash
pnpm run enrich:data
```

This writes cache files into `data/cache/google-places/`, which may be committed for reproducible
enrichment results.

5. Build generated site data:

```bash
pnpm run build:data
```

This writes local generated JSON into `src/data/generated/` from the current contents of `data/raw/`.

6. Run the site:

```bash
pnpm run dev
```

If you already have raw JSON from elsewhere, you can skip source refresh and place compatible files directly in `data/raw/<slug>.json`, then run `pnpm run build:data`.
For a targeted refresh, run `pnpm run sync:source -- <slug-or-url-or-path>`.
For a full forced refresh, run `pnpm run sync:sources:force`.

Legacy aliases still work:
- `pnpm run refresh:data`
- `pnpm run refresh:data:force`
- `pnpm run refresh:data:list -- <slug-or-url>`

## Data Model

The project keeps three layers separate:

1. `data/raw/` stores disposable scraper output.
2. `data/cache/google-places/` stores cached Google Places lookups keyed by stable place id and may be committed.
3. `src/data/overrides/` stores handwritten metadata, tags, notes, and ranking.
4. `src/data/generated/` stores the static JSON that Astro reads at build time.

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
