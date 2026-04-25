# Favorite Places

Static-first travel guides built from Google Maps saved lists.

Favorite Places turns personal saved lists into a small shareable site: guide pages, searchable place cards, map views, top picks, tags, notes, and lightweight theming. It is designed as a template app with user-owned content kept in a separate `site/` pack.

## What You Get

- An Astro static site for browsing guides and places
- A Python + `uv` data pipeline for importing Google Maps lists
- A `site/` overlay for your config, theme, source lists, raw data, overrides, templates, and assets
- Optional Google Places enrichment for categories, status, Maps links, ratings, and photos
- A tiny committed `site.example/` pack that works as a demo

## Quick Start

Install dependencies:

```bash
bun install
uv sync
```

Preview the committed example site pack:

```bash
FAVORITE_PLACES_SITE_DIR=site.example bun run build:data
FAVORITE_PLACES_SITE_DIR=site.example bun run dev
```

Then open the local URL printed by Astro.

The example pack uses two public Google Maps example lists, five places, manual overrides, tags, top picks, and template aside blocks. It does not need API keys to render.

## Create Your Site

Copy the example pack into the default `site/` location:

```bash
cp -R site.example site
```

Edit:

- `site/config.ts` for site name, nav, copy, labels, map provider, and display options
- `site/theme.css` for colors, fonts, spacing, and custom styling
- `site/list_sources.json` for your Google Maps list sources
- `site/overrides/` for handwritten descriptions, tags, notes, top picks, and ranking
- `site/content/templates/` for optional trusted HTML insertion points
- `site/public/` for logos, favicons, and site-owned assets

After `site/` exists, it is the default. You only need `FAVORITE_PLACES_SITE_DIR` when previewing `site.example`, using a private sibling site pack, or using a later `app/ + site/` layout.

## Add Sources

Each source in `site/list_sources.json` needs a stable `slug`.

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

Supported sources:

- Public Google Maps saved-list URLs, including `https://maps.app.goo.gl/...` shortlinks
- Google Takeout saved-list CSV exports

Google My Maps URLs are not supported yet.

## Build Your Data

Refresh configured sources and rebuild generated site data:

```bash
bun run sync:sources
```

Rebuild generated site data from existing raw snapshots:

```bash
bun run build:data
```

Run the site:

```bash
bun run dev
```

Verify before shipping:

```bash
bun run check
bun run build
```

## Optional Enrichment

The site works without Google Places enrichment. If you want richer metadata, add a server/build-time key in `.env`:

```bash
GOOGLE_PLACES_API_KEY=...
```

Then run one of:

```bash
bun run fill:gaps
bun run enrich:data
bun run refresh:enrichment
```

Manual overrides always win over machine-enriched fields.

## Common Environment Variables

```bash
# Browser Google Maps display key.
GOOGLE_MAPS_JS_API_KEY=...

# Server/build-time enrichment fallback.
GOOGLE_PLACES_API_KEY=...

# Force Leaflet/OpenStreetMap rendering.
PUBLIC_MAP_PROVIDER=leaflet

# Hide place photos in the UI.
PUBLIC_PLACE_PHOTOS=off

# Point at a non-default site pack.
FAVORITE_PLACES_SITE_DIR=../site
```

Use a restricted browser key for `GOOGLE_MAPS_JS_API_KEY`. Do not expose `GOOGLE_PLACES_API_KEY` to the browser.

## Deploy

This is a static Astro site, so it can deploy to Cloudflare Pages, GitHub Pages, or similar static hosts.

For Cloudflare Pages, use the custom build path documented in [Architecture](docs/ARCHITECTURE.md#cloudflare-pages). Do not rely on Cloudflare's automatic Python dependency detection for this repo.

## More Docs

- [Architecture](docs/ARCHITECTURE.md): site-pack contract, Cloudflare Pages, refresh automation, data model, and enrichment internals
- [Design System](docs/design-system.md): current visual system notes
