# AGENTS.md

## Project Summary

`favorite-places` is a static-first Astro site that turns personal Google Maps saved lists into cleaner shareable guides.

- Frontend: Astro + TypeScript
- Data pipeline: Python + `uv`
- Source scraper: vendored git subtree at `vendor/google-saved-lists/` from `https://github.com/michaelmwu/google-saved-list-scraper`
- Hosting target: static deploys on Cloudflare Pages or GitHub Pages

## Working Rules

- Prefer `rg` for search.
- Keep the root [`.python-version`](.python-version) aligned with `pyproject.toml` because Cloudflare Pages otherwise falls back to its default Python runtime.
- For Cloudflare Pages deploys, set `SKIP_DEPENDENCY_INSTALL=true` and `BUN_VERSION=1.3.12`, then use a custom build command that runs `bun ci`, installs `uv==0.11.6`, runs `uv sync`, then `bun run build:data` before `bun run build`.
- Use `uv sync` for Python dependencies.
- Use `bun install` for frontend dependencies.
- Use `bun run check` and `bun run build` before closing out frontend changes.
- Use `.venv/bin/python` if `uv run` hits sandbox cache issues in Codex.

## Data Practices

This repo deliberately avoids committing generated build artifacts.

- `data/raw/` may be committed when you want reproducible scraped snapshots in git.
- `data/cache/google-places/` may be committed when you want reproducible enrichment snapshots in git.
- `src/data/generated/` is local-only generated site input data.
- `src/data/overrides/` is the source-controlled layer for handwritten curation.
- `scripts/config/list_sources.json` is safe to commit if it only contains public list URLs you are comfortable sharing.
- In `scripts/config/list_sources.json`, `slug` is required. `type` is inferred for supported Google Maps `url` sources and local CSV `path` sources; only set it explicitly when inference is not enough. Google My Maps URLs are not supported yet. `google_export_csv` sources still require `title`; `title` is optional for Google Maps URL sources as a fallback if the source data cannot recover the real title.

Merge precedence:

1. Manual overrides
2. Google Places enrichment cache
3. Raw scraped list data

## Common Commands

Install dependencies:

```bash
uv sync
bun install
```

Populate local raw data from public Google Maps lists:

```bash
bun run sync:sources
```

This refreshes every configured source and then rebuilds generated site data. URL sources skip network refreshes until their `refresh_after` window expires unless the source config changes; CSV sources skip rewrites when their input hash is unchanged.

Force-refresh all configured raw source imports:

```bash
bun run sync:sources:force
```

Refresh one configured raw source by slug, source URL, or source path:

```bash
bun run sync:source -- tokyo-japan
```

Build generated JSON from local raw data:

```bash
bun run build:data
```

Use this when raw snapshots are already current and you only need to regenerate site inputs.
Configured local CSV sources are auto-imported into `data/raw/<slug>.json` before rebuild. Public URL sources are not refreshed here.

Fill or refresh Google Places enrichment cache:

```bash
GOOGLE_PLACES_API_KEY=... bun run enrich:data
GOOGLE_PLACES_API_KEY=... bun run refresh:enrichment
```

Run the site:

```bash
bun run dev
```

## Notes For Future Agents

- Cloudflare Pages should not use its default `pip install .` auto-detection for this repo; the data pipeline depends on `uv` to resolve the vendored scraper from `vendor/google-saved-lists`.
- Raw place `note` should flow through as the default place note.
- Raw place `is_favorite` should flow through as the default top-pick signal.
- Manual `note` and `top_pick` overrides still win.
- Google Places cache entries now carry `input_signature` and `refresh_after`; invalidation is not a single global TTL anymore.
- Raw saved-list snapshots now carry `fetched_at`, `refresh_after`, and `source_signature`; URL sources skip network refreshes until the refresh window expires unless the source config changes, while CSV sources can skip rewrites when the input hash is unchanged.
- Do not reintroduce tracked generated JSON unless the user explicitly asks for fixture-style examples in git.
