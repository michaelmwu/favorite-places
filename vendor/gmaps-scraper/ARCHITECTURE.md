# gmaps-scraper Architecture

## Place Page Extraction Strategy

`gmaps-scraper` treats Google Maps as an unstable HTML application, not a
stable data API. The extractor therefore prioritizes provenance over simply
finding text that looks right.

The preferred extraction order is:

1. Structured DOM rows from the loaded Google Maps place page.
2. Internal preview payloads when the DOM is incomplete or unavailable.
3. Plain-text fallback from page snapshots as a last resort.

Each lower layer must be more conservative than the layer above it. A fallback
should prevent obvious bad data from being stored; it should not infer business
facts from nearby prose.

## Structured DOM Rows

The browser extractor runs in the page and first targets Google Maps detail
rows. For example, address and website usually appear as `button` or `a`
elements with `data-item-id` values such as `address`, `authority`, `phone:*`,
or `oloc`.

This is the strongest source because the row shape says what the value means.
The extractor may also use stable row structure plus the Google Maps icon glyph
when labels are localized. It should not depend on English-only `aria-label`
prefixes such as `Address:` because pages can render localized labels even when
the requested URL includes `hl=en`.

## Preview Payloads

Preview payloads are Google Maps internal `/maps/preview/place` responses or
embedded data blobs discovered while loading a place page. They are nested
JSON-like arrays, not a public schema.

These payloads can contain useful values when the rendered DOM is thin, blocked,
or only partially available. Because their shape is undocumented, extraction
from preview payloads should favor strongly typed signals: coordinates, plus
codes, phone-looking fields, postal addresses, and compact address arrays.

## Legacy Payloads

Legacy payloads are older scraper/cache records or broad page snapshots that do
not preserve DOM-row provenance. In practice this means fields such as raw body
text, line lists, or historical `address` strings that were already extracted
before the current structured row strategy existed.

The sanitizer handles these records so existing downstream caches can be
refreshed safely. It must be stricter than the structured extractor because it
cannot tell whether a short line came from an address row, a review, a category
chip, or a service-option row.

## Address Policy

Address extraction should prefer explicit address rows. A value from a
structured `data-item-id="address"` row can be accepted even when it is
locality-only, such as `Baku, Azerbaijan`, because the DOM row provides the
address meaning.

Plain-text address fallback is intentionally narrow:

- Accept postal-code, plus-code, digit-plus-locality, and street-keyword
  addresses.
- Accept locality-only strings only when they are compact and have a geographic
  signal, such as a country/subdivision name or region abbreviation.
- Reject URLs, entity tokens, ratings/review counts, status text, phone numbers,
  service-option rows, categories, and review snippets.
- Keep plus codes separate from formatted addresses. A plus code can be useful
  location metadata, but it should not overwrite a real address unless the
  caller deliberately chooses that fallback.

## Adding Fallbacks

Before adding a regex or denylist entry, prefer a structural extractor change.
A fallback is appropriate only when there is a fixture showing why structured
extraction cannot see the value.

New fallback behavior should include:

- A fixture or unit test with the real Google Maps text/HTML shape.
- A comment explaining which layer it protects and why structural extraction is
  insufficient.
- A negative test for nearby UI text that could otherwise be misclassified.

Avoid broad heuristics such as "any short comma-separated phrase is an address".
Those rules tend to convert reviews like `good food, friendly owner` or service
options like `Dine-in, Takeout, Delivery` into addresses.

## LLM Use

LLMs can be useful as an offline analysis tool for proposing selectors when
Google changes the page. They should not run per page in the scraper's normal
path. The durable output should be deterministic extraction rules plus fixtures
that can be reviewed, tested, and rerun without model cost or variability.
