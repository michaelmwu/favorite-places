# Favorite Places

Static-first personal travel guides built from Google Maps saved lists.

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
pnpm run refresh:data
```

Build generated site data from local raw JSON:

```bash
pnpm run build:data
```

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
pnpm dev
```

## Populate Base Data

This repo can commit raw scraped list snapshots in `data/raw/` when you want reproducible source data in git.
It still does not commit local cache files or generated build data.

1. Add your public Google Maps list URLs to `scripts/config/list_sources.json`.

Only `slug` and `url` are required. `title` is optional and acts as a fallback if the
scraper cannot recover the list title.

Example:

```json
[
  {
    "slug": "tokyo-japan",
    "url": "https://maps.app.goo.gl/your-public-list"
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

2. Pull raw list data from your sibling scraper repo:

```bash
pnpm run refresh:data
```

This writes local JSON files into `data/raw/`.

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

This writes local cache files into `data/cache/google-places/`.

5. Build generated site data:

```bash
pnpm run build:data
```

This writes local generated JSON into `src/data/generated/`.

6. Run the site:

```bash
pnpm dev
```

If you already have raw JSON from elsewhere, you can skip the live scrape and place compatible files directly in `data/raw/<slug>.json`, then run `pnpm run build:data`.

## Data Model

The project keeps three layers separate:

1. `data/raw/` stores disposable scraper output.
2. `data/cache/google-places/` stores cached Google Places lookups keyed by stable place id.
3. `src/data/overrides/` stores handwritten metadata, tags, notes, and ranking.
4. `src/data/generated/` stores the static JSON that Astro reads at build time.

Manual overrides always win over machine-enriched fields.

## Google Places Enrichment

Enrichment is optional and cached. A normal build never calls Google.

- `--enrich` fills missing or stale cache entries older than 30 days.
- `--refresh-enrichment` ignores the 30-day cache window and refetches every place.
- Manual overrides still win over Google data.

The current enrichment pass uses Google Places Text Search with a narrow field mask and
location bias around the scraped coordinates. It is meant to fill in useful metadata
such as category, Maps URI, and business status without turning the site build into a
runtime dependency on Google.
