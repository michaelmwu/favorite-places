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

This re-scrapes configured source lists if needed and then rebuilds generated site data.

Force-refresh raw list scrapes even if the saved snapshot is still fresh:

```bash
pnpm run sync:sources:force
```

Force-refresh one configured raw list by slug or source URL:

```bash
pnpm run sync:source -- tokyo-japan
pnpm run sync:source -- https://maps.app.goo.gl/your-public-list
```

Build generated site data from local raw JSON:

```bash
pnpm run build:data
```

Use this when `data/raw/` is already up to date and you only want to regenerate site inputs.

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

1. Add your public Google Maps list URLs to `scripts/config/list_sources.json`.

If you are starting from this repo as a base template, copy the example file first:

```bash
cp scripts/config/list_sources.example.json scripts/config/list_sources.json
```

Only `slug` and `url` are required. `title` is optional and acts as a fallback if the
scraper cannot recover the list title.
`refresh_days` is optional and controls how long a raw scrape stays fresh before `pnpm run sync:sources`
tries to scrape it again. The default is 14 days.

Example:

```json
[
  {
    "slug": "tokyo-japan",
    "url": "https://maps.app.goo.gl/your-public-list",
    "refresh_days": 14
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

2. Pull raw list data through the installed scraper dependency:

```bash
pnpm run sync:sources
```

This writes local JSON files into `data/raw/`, including scrape metadata like `fetched_at`,
`refresh_after`, and a source signature so future refreshes can skip fresh lists. It also rebuilds
the generated site JSON afterward.

3. Add manual curation files in `src/data/overrides/`.

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

If you already have raw JSON from elsewhere, you can skip the live scrape and place compatible files directly in `data/raw/<slug>.json`, then run `pnpm run build:data`.
For a full raw re-scrape even within the freshness window, run
`pnpm run sync:sources:force`.
For a targeted forced re-scrape, run `pnpm run sync:source -- <slug-or-url>`.

Legacy aliases still work:
- `pnpm run refresh:data`
- `pnpm run refresh:data:force`
- `pnpm run refresh:data:list -- <slug-or-url>`

## Template-Ready Files

This repo can keep personal data and still act as the basis for a cleaner template extraction later.
The key is to keep "replace me" files obvious and colocated with the real paths future users will edit.

- `scripts/config/list_sources.json` is your real source list config.
- `scripts/config/list_sources.example.json` is the starter file for template users.
- `src/data/site.ts` is the site-level branding and copy config for this instance.
- `src/data/site.example.ts` shows the expected shape for a new instance.
- `src/data/overrides/lists/*.json` and `src/data/overrides/places/*.json` are real handwritten curation files.
- `src/data/overrides/lists/list.example.json` and `src/data/overrides/places/list.example.json` show the expected override shapes.

For future extraction into a dedicated template repo, the split is:

- Engine: `scripts/`, `src/lib/`, `src/components/`, and Astro wiring.
- Content: `scripts/config/list_sources.json`, `data/raw/`, and `src/data/overrides/`.
- Theme and branding: `src/data/site.ts` plus any styling and assets under `src/styles/` and `public/`.

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
