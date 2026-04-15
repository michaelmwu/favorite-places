import type { Guide, GuideManifest } from "./types";

const guideModules = import.meta.glob("../data/generated/lists/*.json", {
  eager: true,
});

function isGuideModule(value: unknown): value is { default: Guide } {
  return typeof value === "object" && value !== null && "default" in value;
}

const countryNameAliases: Record<string, string> = {
  "2 Chome1917 RS-ONE 1F": "Japan",
  Korea: "South Korea",
  "New York": "United States",
  Okinawa: "Japan",
  "Singapore 238252": "Singapore",
  UAE: "United Arab Emirates",
  UK: "United Kingdom",
  USA: "United States",
  "モナコ": "Monaco",
};

const countryNameBySlug: Record<string, string> = {
  seychelles: "Seychelles",
};

function getDisplayCountryName(guide: Guide): string {
  if (guide.slug in countryNameBySlug) {
    return countryNameBySlug[guide.slug];
  }

  return countryNameAliases[guide.country_name] ?? guide.country_name;
}

function summarizeGuide(guide: Guide): GuideManifest {
  const featuredPlaceIds = new Set(guide.featured_place_ids);
  const featuredNames = guide.places
    .filter((place) => featuredPlaceIds.has(place.id) && !place.hidden)
    .map((place) => place.name)
    .slice(0, 3);

  return {
    slug: guide.slug,
    title: guide.title,
    description: guide.description,
    country_name: getDisplayCountryName(guide),
    country_code: guide.country_code,
    city_name: guide.city_name,
    list_tags: guide.list_tags,
    place_count: guide.place_count,
    featured_names: featuredNames,
    top_categories: guide.top_categories,
  };
}

export function getGuideManifests(): GuideManifest[] {
  return getGuides()
    .map(summarizeGuide)
    .sort((a, b) => a.title.localeCompare(b.title));
}

export function getGuides(): Guide[] {
  return Object.values(guideModules)
    .filter(isGuideModule)
    .map((module) => module.default)
    .sort((a, b) => a.title.localeCompare(b.title));
}

export function getGuideBySlug(slug: string): Guide | undefined {
  return getGuides().find((guide) => guide.slug === slug);
}
