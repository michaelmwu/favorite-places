import { siteConfig } from "../data/site";
import type { Guide, GuideManifest, Place } from "./types";

export type SeoJsonLd = Record<string, unknown>;

const DEFAULT_DESCRIPTION_LENGTH = 160;

function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function stripMarkdownLinks(value: string): string {
  return value
    .replace(/\[([^\]\n]+)\]\((https?:\/\/[^\s)]+)\)/g, "$1")
    .replace(/https?:\/\/\S+/g, "");
}

function formatList(values: string[]): string {
  if (values.length === 0) {
    return "";
  }

  if (values.length === 1) {
    return values[0];
  }

  if (values.length === 2) {
    return `${values[0]} and ${values[1]}`;
  }

  return `${values.slice(0, -1).join(", ")}, and ${values.at(-1)}`;
}

function formatCategoryLabel(value: string): string {
  return normalizeWhitespace(value.replace(/-/g, " "));
}

function truncateDescription(value: string, maxLength = DEFAULT_DESCRIPTION_LENGTH): string {
  const normalized = normalizeWhitespace(value);
  if (normalized.length <= maxLength) {
    return normalized;
  }

  const cutoff = normalized.lastIndexOf(" ", maxLength - 3);
  const sliceEnd = cutoff >= Math.floor(maxLength * 0.6) ? cutoff : maxLength - 3;
  return `${normalized.slice(0, sliceEnd).trimEnd()}...`;
}

export function toPlainText(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }

  const normalized = normalizeWhitespace(stripMarkdownLinks(value));
  return normalized || null;
}

export function buildHomeMetaDescription({
  intro,
  guideCount,
  countryCount,
  placeCount,
}: {
  intro: string;
  guideCount: number;
  countryCount: number;
  placeCount: number;
}): string {
  return truncateDescription(
    `${toPlainText(intro) ?? siteConfig.siteDescription} Browse ${guideCount} guides across ${countryCount} countries and ${placeCount} saved places.`,
  );
}

export function buildGuideMetaDescription({
  guide,
  countryName,
  featuredPlaceNames,
}: {
  guide: Guide;
  countryName: string;
  featuredPlaceNames: string[];
}): string {
  const explicitDescription = toPlainText(guide.description);
  if (explicitDescription) {
    return truncateDescription(explicitDescription);
  }

  const location = [guide.city_name, countryName].filter(Boolean).join(", ");
  const placeLabel = guide.place_count === 1 ? "place" : "places";
  const featuredSnippet = featuredPlaceNames.length
    ? ` Includes ${formatList(featuredPlaceNames.slice(0, 3))}.`
    : "";
  const categorySnippet =
    !featuredSnippet && guide.top_categories.length
      ? ` Highlights ${formatList(guide.top_categories.slice(0, 3).map(formatCategoryLabel))}.`
      : "";

  return truncateDescription(
    `${guide.place_count} saved ${placeLabel} in ${location}.${featuredSnippet}${categorySnippet}`,
  );
}

function buildPublisher(): SeoJsonLd | undefined {
  const publisherName = normalizeWhitespace(siteConfig.ownerName);
  if (!publisherName) {
    return undefined;
  }

  return {
    "@type": "Person",
    name: publisherName,
  };
}

export function buildWebsiteJsonLd(siteUrl: string): SeoJsonLd {
  const publisher = buildPublisher();

  return {
    "@context": "https://schema.org",
    "@type": "WebSite",
    "@id": `${siteUrl}#website`,
    name: siteConfig.siteName,
    url: siteUrl,
    description: siteConfig.siteDescription,
    ...(publisher ? { publisher } : {}),
  };
}

export function buildHomeJsonLd({
  siteUrl,
  pageUrl,
  description,
  guides,
}: {
  siteUrl: string;
  pageUrl: string;
  description: string;
  guides: GuideManifest[];
}): SeoJsonLd {
  return {
    "@context": "https://schema.org",
    "@type": "CollectionPage",
    "@id": `${pageUrl}#webpage`,
    url: pageUrl,
    name: siteConfig.siteName,
    description,
    isPartOf: {
      "@id": `${siteUrl}#website`,
    },
    mainEntity: {
      "@type": "ItemList",
      numberOfItems: guides.length,
      itemListElement: guides.slice(0, 24).map((guide, index) => ({
        "@type": "ListItem",
        position: index + 1,
        url: new URL(`/guides/${guide.slug}/`, siteUrl).toString(),
        name: guide.title,
        description:
          toPlainText(guide.description) ??
          truncateDescription(
            `${guide.place_count} saved places in ${[guide.city_name, guide.country_name].filter(Boolean).join(", ")}.`,
          ),
      })),
    },
  };
}

function buildPlaceEntity(place: Place): SeoJsonLd {
  return {
    "@type": "Place",
    name: place.name,
    ...(place.address ? { address: place.address } : {}),
    ...(place.maps_url ? { sameAs: place.maps_url } : {}),
    ...(toPlainText(place.note ?? place.why_recommended)
      ? { description: toPlainText(place.note ?? place.why_recommended) }
      : {}),
    ...(typeof place.lat === "number" && typeof place.lng === "number"
      ? {
          geo: {
            "@type": "GeoCoordinates",
            latitude: place.lat,
            longitude: place.lng,
          },
        }
      : {}),
  };
}

export function buildGuideJsonLd({
  siteUrl,
  pageUrl,
  description,
  guide,
  countryName,
  visiblePlaces,
}: {
  siteUrl: string;
  pageUrl: string;
  description: string;
  guide: Guide;
  countryName: string;
  visiblePlaces: Place[];
}): SeoJsonLd[] {
  return [
    {
      "@context": "https://schema.org",
      "@type": "BreadcrumbList",
      itemListElement: [
        {
          "@type": "ListItem",
          position: 1,
          name: siteConfig.siteName,
          item: siteUrl,
        },
        {
          "@type": "ListItem",
          position: 2,
          name: guide.title,
          item: pageUrl,
        },
      ],
    },
    {
      "@context": "https://schema.org",
      "@type": "CollectionPage",
      "@id": `${pageUrl}#webpage`,
      url: pageUrl,
      name: guide.title,
      description,
      isPartOf: {
        "@id": `${siteUrl}#website`,
      },
      about: {
        "@type": "Place",
        name: [guide.city_name, countryName].filter(Boolean).join(", "),
      },
      ...(guide.list_tags.length || guide.top_categories.length
        ? {
            keywords: [...guide.list_tags, ...guide.top_categories]
              .map((value) => normalizeWhitespace(value))
              .filter(Boolean)
              .slice(0, 10)
              .join(", "),
          }
        : {}),
      mainEntity: {
        "@type": "ItemList",
        numberOfItems: visiblePlaces.length,
        itemListElement: visiblePlaces.slice(0, 25).map((place, index) => ({
          "@type": "ListItem",
          position: index + 1,
          item: buildPlaceEntity(place),
        })),
      },
    },
  ];
}
