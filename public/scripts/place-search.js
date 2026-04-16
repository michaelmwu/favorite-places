const STOP_WORDS = new Set([
  "a",
  "an",
  "and",
  "around",
  "at",
  "best",
  "for",
  "in",
  "near",
  "of",
  "on",
  "the",
  "to",
  "with",
]);

const CATEGORY_ALIASES = new Map([
  ["bar", ["bar", "pub", "cocktail"]],
  ["cafe", ["cafe", "cafes", "coffee", "coffee shop", "coffee shops", "espresso"]],
  ["museum", ["museum", "museums", "gallery", "galleries"]],
  ["restaurant", ["restaurant", "restaurants", "dinner", "lunch", "food", "eat", "eats"]],
  ["shop", ["shop", "shops", "store", "stores", "shopping"]],
]);

const CATEGORY_MATCH_TERMS = new Map([
  ["bar", ["bar", "pub", "cocktail"]],
  ["cafe", ["cafe", "coffee", "coffee-shop", "coffee shop", "bakery", "tea"]],
  ["museum", ["museum", "gallery", "art-gallery", "art gallery"]],
  ["restaurant", ["restaurant", "food", "dining", "eatery"]],
  ["shop", ["shop", "store", "market"]],
]);

const VIBE_ALIASES = new Map([
  ["cheap-eats", ["cheap", "affordable", "budget", "inexpensive", "cheap eats"]],
  ["cozy", ["cozy", "cosy", "good vibes", "warm", "homey"]],
  ["date-night", ["date night", "date", "romantic", "special occasion"]],
  ["design-forward", ["design", "stylish", "beautiful", "aesthetic", "good vibes"]],
  ["family-friendly", ["family", "kids", "children"]],
  ["group-friendly", ["group", "friends", "share", "shared"]],
  ["hidden-gem", ["hidden", "hidden gem", "underrated"]],
  ["laptop-friendly", ["laptop", "work", "wifi", "wi fi", "outlets"]],
  ["late-night", ["late night", "late", "nightlife"]],
  ["lively", ["lively", "buzzy", "fun", "good vibes"]],
  ["local-favorite", ["local", "local favorite", "regulars", "good vibes"]],
  ["outdoor-seating", ["outdoor", "patio", "terrace", "outside"]],
  ["quiet", ["quiet", "calm", "peaceful", "chill"]],
  ["rainy-day", ["rainy", "rainy day", "indoor", "indoors"]],
  ["scenic", ["scenic", "view", "views", "waterfront", "rooftop"]],
  ["slow-afternoon", ["slow", "afternoon", "linger"]],
  ["solo-friendly", ["solo", "alone", "counter"]],
  ["splurge", ["splurge", "expensive", "fine dining", "michelin"]],
  ["touristy-but-worth-it", ["touristy", "worth it", "iconic", "landmark"]],
]);

const LOCATION_ALIASES = new Map([
  ["new-york-new-york-usa", ["nyc", "new york city"]],
  ["san-francisco-california-usa", ["sf", "san fran", "san francisco"]],
  ["los-angeles-california-usa", ["la", "los angeles"]],
  ["tokyo-japan", ["tokyo"]],
]);

let cachedIndexPromise;

export async function loadSearchIndex(url = "/data/search-index.json") {
  cachedIndexPromise ??= fetch(url)
    .then((response) => {
      if (!response.ok) {
        throw new Error(`Search index request failed: ${response.status}`);
      }
      return response.json();
    })
    .then(prepareSearchIndex);

  return cachedIndexPromise;
}

export function prepareSearchIndex(index) {
  const guides = Array.isArray(index?.guides) ? index.guides : [];
  const entries = Array.isArray(index?.entries) ? index.entries : Array.isArray(index) ? index : [];
  const normalizedGuides = guides.map((guide) => ({
    ...guide,
    _searchText: normalizeText(guide.search_text || guide.title || ""),
    _aliases: guideAliases(guide),
  }));
  const guideBySlug = new Map(normalizedGuides.map((guide) => [guide.slug, guide]));
  const normalizedEntries = entries.map((entry) => ({
    ...entry,
    tags: Array.isArray(entry.tags) ? entry.tags : [],
    vibe_tags: Array.isArray(entry.vibe_tags) ? entry.vibe_tags : [],
    _searchText: normalizeText(entry.search_text || ""),
    _name: normalizeText(entry.name || ""),
    _category: normalizeText(entry.category || ""),
    _neighborhood: normalizeText(entry.neighborhood || ""),
    _guide: guideBySlug.get(entry.guide_slug),
  }));

  return {
    ...index,
    guides: normalizedGuides,
    entries: normalizedEntries,
    guideBySlug,
  };
}

export function searchPlaces(query, options = {}) {
  const index = options.index ? prepareSearchIndexOnce(options.index) : prepareSearchIndex({ entries: [] });
  const parsed = parseQuery(query, index, options);
  const activeFilters = options.activeFilters || {};
  const scopedEntries = index.entries.filter((entry) => {
    if (options.scope === "guide" && options.guideSlug && entry.guide_slug !== options.guideSlug) {
      return false;
    }
    if (parsed.guideSlugs.size > 0 && !parsed.guideSlugs.has(entry.guide_slug)) {
      return false;
    }
    if (
      activeFilters.tag &&
      !normalizedList([...entry.tags, ...entry.vibe_tags]).includes(normalizeToken(activeFilters.tag))
    ) {
      return false;
    }
    if (
      activeFilters.neighborhood &&
      normalizeToken(entry.neighborhood || "") !== normalizeToken(activeFilters.neighborhood)
    ) {
      return false;
    }
    if (activeFilters.topPicksOnly && !entry.top_pick) {
      return false;
    }
    return true;
  });

  const results = scopedEntries
    .map((entry) => scoreEntry(entry, parsed, options))
    .filter((result) => parsed.tokens.length === 0 || result.score > 0)
    .sort((left, right) => right.score - left.score || curatedSort(left.entry, right.entry));

  return {
    query,
    count: results.length,
    parsed: {
      categories: [...parsed.categories],
      guideSlugs: [...parsed.guideSlugs],
      unmatchedTerms: parsed.unmatchedTerms,
      vibes: [...parsed.vibes],
    },
    results,
  };
}

export function searchGuides(query, options = {}) {
  const index = options.index ? prepareSearchIndexOnce(options.index) : prepareSearchIndex({ guides: [] });
  const normalizedQuery = normalizeText(query);
  const tokens = tokenize(normalizedQuery);
  if (tokens.length === 0) {
    return [];
  }

  return index.guides
    .map((guide) => {
      let score = 0;
      const matchedSignals = [];
      for (const alias of guide._aliases) {
        if (containsPhrase(normalizedQuery, alias)) {
          score += alias === normalizeText(guide.title) ? 40 : 28;
          matchedSignals.push("guide");
        }
      }
      for (const token of tokens) {
        if (guide._searchText.includes(token)) {
          score += 3;
        }
      }
      return { guide, matchedSignals: [...new Set(matchedSignals)], score };
    })
    .filter((result) => result.score > 0)
    .sort((left, right) => right.score - left.score || left.guide.title.localeCompare(right.guide.title));
}

export function normalizeText(value) {
  return String(value || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/&/g, " and ")
    .replace(/['']/g, "")
    .replace(/[^a-z0-9]+/gi, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function parseQuery(query, index, options) {
  const normalizedQuery = normalizeText(query);
  const tokens = tokenize(normalizedQuery);
  const consumedTokens = new Set();
  const categories = new Set();
  const vibes = new Set();
  const guideSlugs = new Set();

  for (const [category, aliases] of CATEGORY_ALIASES) {
    if (aliases.some((alias) => consumePhrase(normalizedQuery, alias, consumedTokens))) {
      categories.add(category);
    }
  }

  for (const [vibe, aliases] of VIBE_ALIASES) {
    if (aliases.some((alias) => consumePhrase(normalizedQuery, alias, consumedTokens))) {
      vibes.add(vibe);
    }
  }

  for (const guide of index.guides) {
    if (guide._aliases.some((alias) => consumePhrase(normalizedQuery, alias, consumedTokens))) {
      guideSlugs.add(guide.slug);
    }
  }

  if (options.scope === "guide" && options.guideSlug) {
    guideSlugs.delete(options.guideSlug);
  }

  const unmatchedTerms = tokens.filter((token) => !STOP_WORDS.has(token) && !consumedTokens.has(token));

  return {
    categories,
    guideSlugs,
    normalizedQuery,
    tokens,
    unmatchedTerms,
    vibes,
  };
}

function scoreEntry(entry, parsed, options) {
  const matchedSignals = [];
  let score = 0;
  const entryTags = normalizedList(entry.tags);
  const entryVibes = normalizedList(entry.vibe_tags);
  const categoryText = normalizeText([entry.category, ...entryTags].join(" "));

  if (options.scope === "guide") {
    score += 4;
  }

  if (parsed.guideSlugs.has(entry.guide_slug)) {
    score += 80;
    matchedSignals.push("location");
  }

  for (const category of parsed.categories) {
    const matchTerms = CATEGORY_MATCH_TERMS.get(category) || [category];
    if (matchTerms.some((term) => categoryText.includes(normalizeText(term)))) {
      score += 38;
      matchedSignals.push("category");
    }
  }

  for (const vibe of parsed.vibes) {
    if (entryVibes.includes(vibe) || entryTags.includes(vibe)) {
      score += 34;
      matchedSignals.push("vibe");
    }
  }

  if (parsed.normalizedQuery && entry._name === parsed.normalizedQuery) {
    score += 55;
    matchedSignals.push("name");
  } else if (parsed.normalizedQuery && entry._name.includes(parsed.normalizedQuery)) {
    score += 30;
    matchedSignals.push("name");
  }

  if (parsed.normalizedQuery && entry._neighborhood.includes(parsed.normalizedQuery)) {
    score += 22;
    matchedSignals.push("neighborhood");
  }

  if (parsed.normalizedQuery && entry._searchText.includes(parsed.normalizedQuery)) {
    score += 18;
    matchedSignals.push("text");
  }

  for (const term of parsed.unmatchedTerms) {
    if (entry._name.includes(term)) {
      score += 12;
      matchedSignals.push("name");
    } else if (entryTags.includes(term) || entryVibes.includes(term) || entry._category.includes(term)) {
      score += 8;
      matchedSignals.push("tag");
    } else if (entry._searchText.includes(term)) {
      score += 3;
      matchedSignals.push("text");
    }
  }

  if (parsed.tokens.length === 0) {
    score = 1;
  }

  if (entry.top_pick) {
    score += 8;
    matchedSignals.push("top-pick");
  }
  if (Number(entry.manual_rank) > 0) {
    score += Math.min(8, Number(entry.manual_rank));
  }

  return {
    entry,
    matchedSignals: [...new Set(matchedSignals)],
    score,
  };
}

function guideAliases(guide) {
  const aliases = new Set([
    guide.slug,
    String(guide.slug || "").replaceAll("-", " "),
    guide.title,
    guide.city,
    guide.country,
    guide.country_code,
    [guide.city, guide.country].filter(Boolean).join(" "),
  ]);
  for (const alias of LOCATION_ALIASES.get(guide.slug) || []) {
    aliases.add(alias);
  }
  return [...aliases].map(normalizeText).filter(Boolean);
}

function tokenize(value) {
  return normalizeText(value)
    .split(" ")
    .filter((token) => token.length > 0);
}

function consumePhrase(query, phrase, consumedTokens) {
  const normalizedPhrase = normalizeText(phrase);
  if (!containsPhrase(query, normalizedPhrase)) {
    return false;
  }
  for (const token of tokenize(normalizedPhrase)) {
    consumedTokens.add(token);
  }
  return true;
}

function containsPhrase(query, phrase) {
  if (!phrase) {
    return false;
  }
  if (phrase.length <= 2) {
    return new RegExp(`(^|\\s)${escapeRegExp(phrase)}($|\\s)`).test(query);
  }
  return new RegExp(`(^|\\s)${escapeRegExp(phrase)}($|\\s)`).test(query);
}

function normalizedList(values) {
  return values.map(normalizeToken).filter(Boolean);
}

function normalizeToken(value) {
  return normalizeText(value).replaceAll(" ", "-");
}

function curatedSort(left, right) {
  const topPickDelta = Number(Boolean(right.top_pick)) - Number(Boolean(left.top_pick));
  if (topPickDelta !== 0) {
    return topPickDelta;
  }
  return Number(right.manual_rank || 0) - Number(left.manual_rank || 0) || left.name.localeCompare(right.name);
}

function prepareSearchIndexOnce(index) {
  if (index?.guideBySlug instanceof Map) {
    return index;
  }
  return prepareSearchIndex(index);
}

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
