const ADDRESS_TAG_PATTERNS = [
  /(^|-)chome($|-)/,
  /(^|-)be-re-/,
  /(^|-)tokyo-\d{3}-\d{4}$/,
  /(^|-)\d{3}-\d{4}$/,
  /(^|-)(bldg|building|tower|plaza|terrace|floor|mansion|palace|gems|gratteciel)($|-)/,
];

const STREET_AREA_PATTERN =
  /^(p\.|d\.|ng\.|c\/|c\.|carrer\b|rda\.|calle\b|av\b|av\.|jl\.|jalan\b|ngo\b|duong\b)/;
const DEFAULT_AREA_FILTER_LIMIT = 12;
const GUIDE_LOCATION_PART_SPLIT_PATTERN = /\s*(?:&|\/|\+|\band\b)\s*/i;
const LOCATION_TAG_ALIASES: Record<string, string[]> = {
  korea: ["kr", "south-korea"],
  "south-korea": ["kr", "korea"],
  "united-arab-emirates": ["ae", "uae"],
  "united-kingdom": ["gb", "uk"],
  "united-states": ["us", "usa"],
  geneve: ["geneva"],
  geneva: ["geneve"],
};

interface AreaFilterPlace {
  neighborhood: string | null;
}

export interface AreaFilter {
  label: string;
  value: string;
  count: number;
}

interface GuideTagContext {
  cityName?: string | null;
  countryCode?: string | null;
  countryName?: string | null;
}

function normalizeAreaText(area: string): string {
  return area
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toLowerCase();
}

export function normalizeTagValue(value: string): string {
  return value
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

export function getTagComparisonValue(value: string): string {
  const normalizedText = value
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");

  return normalizeTagValue(normalizedText) || normalizedText;
}

function getGuideLocationTagsToHide({
  cityName,
  countryCode,
  countryName,
}: GuideTagContext): Set<string> {
  const hiddenTags = new Set<string>();
  const addTag = (value?: string | null) => {
    const normalized = value ? getTagComparisonValue(value) : "";
    if (normalized) {
      hiddenTags.add(normalized);
      (LOCATION_TAG_ALIASES[normalized] ?? []).forEach((alias) => hiddenTags.add(alias));
    }
  };

  addTag(cityName);
  cityName
    ?.split(GUIDE_LOCATION_PART_SPLIT_PATTERN)
    .map((part) => part.trim())
    .filter(Boolean)
    .forEach(addTag);
  addTag(countryName);
  addTag(countryCode);

  return hiddenTags;
}

export function isDisplayPlaceTag(tag: string): boolean {
  const normalizedTag = tag.toLowerCase();
  return !ADDRESS_TAG_PATTERNS.some((pattern) => pattern.test(normalizedTag));
}

export function getDisplayPlaceTags(tags: string[]): string[] {
  return tags.filter(isDisplayPlaceTag);
}

export function getDisplayGuideTags(tags: string[], context: GuideTagContext = {}): string[] {
  const hiddenTags = getGuideLocationTagsToHide(context);
  return tags.filter((tag) => {
    const normalizedTag = getTagComparisonValue(tag);
    return tag.trim().length > 0 && !hiddenTags.has(normalizedTag);
  });
}

export function getGuideAreaFilters(
  places: AreaFilterPlace[],
  { limit = DEFAULT_AREA_FILTER_LIMIT }: { limit?: number } = {},
): AreaFilter[] {
  const totalPlaces = places.length;
  const areaCounts = new Map<string, AreaFilter>();

  places.forEach((place) => {
    const label = place.neighborhood?.trim();
    if (!label) return;

    const normalizedValue = normalizeAreaText(label);
    if (!normalizedValue || STREET_AREA_PATTERN.test(normalizedValue)) return;

    const current = areaCounts.get(normalizedValue);
    if (current) {
      current.count += 1;
      return;
    }

    areaCounts.set(normalizedValue, {
      label,
      value: getTagComparisonValue(label),
      count: 1,
    });
  });

  return [...areaCounts.values()]
    .filter((area) => area.count < totalPlaces)
    .sort((left, right) => right.count - left.count || left.label.localeCompare(right.label))
    .slice(0, limit);
}
