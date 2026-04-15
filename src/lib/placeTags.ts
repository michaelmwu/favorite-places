const ADDRESS_TAG_PATTERN =
  /(^|-)chome($|-)|(^|-)be-re-|\d|\b(bldg|building|tower|plaza|terrace|floor|mansion|palace|gems|gratteciel|road|rd|street|st|lane|ln|avenue|ave)\b/;

export function isDisplayPlaceTag(tag: string): boolean {
  return !ADDRESS_TAG_PATTERN.test(tag.toLowerCase());
}

export function getDisplayPlaceTags(tags: string[]): string[] {
  return tags.filter(isDisplayPlaceTag);
}
