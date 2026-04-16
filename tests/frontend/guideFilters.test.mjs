import { describe, expect, it } from "vitest";

import {
  buildAreaFilterStatusMessage,
  buildEmptyStateMessage,
  cardHasTag,
  countMatchingCards,
} from "../../public/scripts/guide-filters.js";

const makeCard = ({
  neighborhood = "",
  placeId,
  search = "",
  tags = "",
  vibeTags = "",
}) => ({
  dataset: {
    neighborhood,
    placeId,
    search,
    tags,
    vibeTags,
  },
});

describe("guide filters", () => {
  it("counts vibe matches within the active area", () => {
    const cards = [
      makeCard({ placeId: "1", neighborhood: "south brisbane", search: "coffee brunch", vibeTags: "date-night scenic" }),
      makeCard({ placeId: "2", neighborhood: "south brisbane", search: "rain museum", vibeTags: "rainy-day" }),
      makeCard({ placeId: "3", neighborhood: "west end", search: "sunset drinks", vibeTags: "date-night scenic" }),
    ];

    expect(countMatchingCards(cards, {
      activeArea: "south brisbane",
      normalizedQuery: "",
      tag: "date-night",
    })).toBe(1);
    expect(countMatchingCards(cards, {
      activeArea: "south brisbane",
      normalizedQuery: "",
      tag: "",
    })).toBe(2);
  });

  it("matches tags across place and vibe tag fields", () => {
    const card = makeCard({ placeId: "1", tags: "coffee bakery", vibeTags: "cozy scenic" });

    expect(cardHasTag(card, "bakery")).toBe(true);
    expect(cardHasTag(card, "scenic")).toBe(true);
    expect(cardHasTag(card, "date-night")).toBe(false);
  });

  it("builds area-aware status and empty messages when broader matches exist", () => {
    expect(buildAreaFilterStatusMessage({
      activeAreaLabel: "South Brisbane",
      visibleCount: 1,
      overflowCount: 3,
    })).toBe("Showing 1 place in South Brisbane. 3 more match elsewhere in this guide.");

    expect(buildEmptyStateMessage({
      activeAreaLabel: "South Brisbane",
      overflowCount: 2,
      query: "",
    })).toBe("No matches in South Brisbane. 2 more match elsewhere in this guide. Try another area or clear the area filter.");
  });
});
