const ADDRESS_TAG_PATTERNS = [
  /(^|-)chome($|-)/,
  /(^|-)be-re-/,
  /(^|-)tokyo-\d{3}-\d{4}$/,
  /(^|-)\d{3}-\d{4}$/,
  /(^|-)(bldg|building|tower|plaza|terrace|floor|mansion|palace|gems|gratteciel)($|-)/,
];

export function isDisplayPlaceTag(tag: string): boolean {
  const normalizedTag = tag.toLowerCase();
  return !ADDRESS_TAG_PATTERNS.some((pattern) => pattern.test(normalizedTag));
}

export function getDisplayPlaceTags(tags: string[]): string[] {
  return tags.filter(isDisplayPlaceTag);
}
