# AGENTS.md

## Project Summary

`favorite-places` is a static-first Astro site that turns personal Google Maps saved lists into cleaner shareable guides.

- Frontend: Astro + TypeScript
- Data pipeline: Python + `uv`
- Source scraper: public repo at `https://github.com/michaelmwu/google-saved-list-scraper`
- Hosting target: static deploys on Cloudflare Pages or GitHub Pages

## Working Rules

- Prefer `rg` for search.
- Use `uv sync` for Python dependencies.
- Use `pnpm install` for frontend dependencies.
- Use `pnpm run check` and `pnpm run build` before closing out frontend changes.
- Use `.venv/bin/python` if `uv run` hits sandbox cache issues in Codex.

## Data Practices

This repo deliberately avoids committing generated build artifacts.

- `data/raw/` may be committed when you want reproducible scraped snapshots in git.
- `data/cache/google-places/` may be committed when you want reproducible enrichment snapshots in git.
- `src/data/generated/` is local-only generated site input data.
- `src/data/overrides/` is the source-controlled layer for handwritten curation.
- `scripts/config/list_sources.json` is safe to commit if it only contains public list URLs you are comfortable sharing.
- In `scripts/config/list_sources.json`, `slug` and `url` are required. `title` is only a fallback if the scraper cannot recover the real title. `refresh_days` is optional.

Merge precedence:

1. Manual overrides
2. Google Places enrichment cache
3. Raw scraped list data

## Common Commands

Install dependencies:

```bash
uv sync
pnpm install
```

Populate local raw data from public Google Maps lists:

```bash
pnpm run sync:sources
```

This refreshes raw sources when needed and then rebuilds generated site data.

Force-refresh all configured raw list scrapes:

```bash
pnpm run sync:sources:force
```

Force-refresh one configured raw list by slug or source URL:

```bash
pnpm run sync:source -- tokyo-japan
```

Build generated JSON from local raw data:

```bash
pnpm run build:data
```

Use this when raw snapshots are already current and you only need to regenerate site inputs.

Fill or refresh Google Places enrichment cache:

```bash
GOOGLE_PLACES_API_KEY=... pnpm run enrich:data
GOOGLE_PLACES_API_KEY=... pnpm run refresh:enrichment
```

Run the site:

```bash
pnpm run dev
```

## Notes For Future Agents

- Raw place `note` should flow through as the default place note.
- Raw place `is_favorite` should flow through as the default top-pick signal.
- Manual `note` and `top_pick` overrides still win.
- Google Places cache entries now carry `input_signature` and `refresh_after`; invalidation is not a single global TTL anymore.
- Raw saved-list snapshots now carry `fetched_at`, `refresh_after`, and `source_signature` so `--refresh` can skip recent scrapes.
- Do not reintroduce tracked generated JSON unless the user explicitly asks for fixture-style examples in git.
