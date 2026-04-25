# Design System

This repo now uses Tailwind CSS 4 as a styling engine and keeps the project-specific visual language in a small internal design system.

## Principles

- Keep Astro-first rendering. Do not introduce React-only component libraries just to style static pages.
- Prefer semantic `ui-*` classes and small Astro primitives for repeated patterns.
- Use one-off Tailwind utilities in markup for local layout adjustments, not to re-encode the entire visual system inline.
- Keep existing JS hook classes and `data-*` attributes stable unless the behavior code changes with them.

## Theme Tokens

Theme tokens live in [src/styles/theme.css](../src/styles/theme.css).

- Fonts: `font-sans`, `font-display`
- Colors: `app-canvas`, `app-panel`, `app-panel-muted`, `app-ink`, `app-muted`, `app-line`, `app-brand`, `app-brand-soft`, `app-brand-strong`
- Radius: `rounded-card`, `rounded-control`, `rounded-pill`
- Shadows: `shadow-card`, `shadow-floating`

These map back to the existing CSS custom properties so the visual language stays consistent while Tailwind utilities become available.

## Core Primitives

Core semantic classes live in [src/styles/design-system.css](../src/styles/design-system.css).

- Layout: `ui-site-header`, `ui-page-shell`, `ui-hero`, `ui-hero-grid`, `ui-section`
- Typography: `ui-eyebrow`, `ui-display`, `ui-section-title`, `ui-copy`, `ui-copy--sm`
- Actions and metadata: `ui-pill`, `ui-pill--brand`, `ui-tag-pill`, `ui-meta-row`
- Surfaces: `ui-panel`, `ui-control-panel`, `ui-card`, `ui-card-link`, `ui-card-footer`
- Feedback: `ui-empty-state`, `ui-back-link`

## Astro Components

- [src/components/ui/SectionHeading.astro](../src/components/ui/SectionHeading.astro): shared heading block for section eyebrow/title/copy patterns.

Use a small Astro component when a pattern carries structure and semantics. Use a semantic CSS class when the pattern is mostly visual.

## Migration Guidance

- New shared UI should prefer `ui-*` classes first.
- Existing legacy classes can remain where scripts or older styles still depend on them.
- When touching older markup, add the `ui-*` counterpart rather than inventing a new ad hoc class.
