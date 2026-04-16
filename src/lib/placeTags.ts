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

interface AreaFilterPlace {
  neighborhood: string | null;
}

export interface AreaFilter {
  label: string;
  value: string;
  count: number;
}

function normalizeAreaText(area: string): string {
  return area
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toLowerCase();
}

export function isDisplayPlaceTag(tag: string): boolean {
  const normalizedTag = tag.toLowerCase();
  return !ADDRESS_TAG_PATTERNS.some((pattern) => pattern.test(normalizedTag));
}

export function getDisplayPlaceTags(tags: string[]): string[] {
  return tags.filter(isDisplayPlaceTag);
}

export function getGuideAreaFilters(
  places: AreaFilterPlace[],
  { limit = DEFAULT_AREA_FILTER_LIMIT }: { limit?: number } = {},
): AreaFilter[] {
  const totalPlaces = places.length;
  const minimumCount = totalPlaces >= 20 ? 3 : 2;
  const areaCounts = new Map<string, AreaFilter>();

  places.forEach((place) => {
    const label = place.neighborhood?.trim();
    if (!label) return;

    const normalizedValue = normalizeAreaText(label);
    if (!normalizedValue || STREET_AREA_PATTERN.test(normalizedValue)) return;

    const value = label.toLowerCase();
    const current = areaCounts.get(value);
    if (current) {
      current.count += 1;
      return;
    }

    areaCounts.set(value, { label, value, count: 1 });
  });

  return [...areaCounts.values()]
    .filter((area) => area.count >= minimumCount && area.count < totalPlaces)
    .sort((left, right) => right.count - left.count || left.label.localeCompare(right.label))
    .slice(0, limit);
}
