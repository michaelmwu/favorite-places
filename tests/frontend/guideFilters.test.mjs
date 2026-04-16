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
    const card = makeCard({
      placeId: "1",
      tags: JSON.stringify(["coffee", "bakery"]),
      vibeTags: JSON.stringify(["Date Night", "scenic"]),
    });

    expect(cardHasTag(card, "bakery")).toBe(true);
    expect(cardHasTag(card, "scenic")).toBe(true);
    expect(cardHasTag(card, "date-night")).toBe(true);
  });

  it("builds area-aware status and empty messages when broader matches exist", () => {
    expect(buildAreaFilterStatusMessage({
      activeAreaLabel: "South Brisbane",
      visibleCount: 1,
      overflowCount: 3,
    })).toBe("Showing 1 place in South Brisbane. 3 more matches elsewhere in this guide.");

    expect(buildEmptyStateMessage({
      activeAreaLabel: "South Brisbane",
      overflowCount: 2,
      query: "",
    })).toBe("No matches in South Brisbane. 2 more matches elsewhere in this guide. Try another area or clear the area filter.");
  });

  it("keeps generic empty-state copy when no area filter is active", () => {
    expect(buildEmptyStateMessage({
      activeAreaLabel: "",
      overflowCount: 2,
      query: "",
    })).toBe("No matches. Try a broader search or clear the tag filter.");
  });

  it("uses singular overflow grammar for a single broader result", () => {
    expect(buildAreaFilterStatusMessage({
      activeAreaLabel: "South Brisbane",
      visibleCount: 1,
      overflowCount: 1,
    })).toBe("Showing 1 place in South Brisbane. 1 more match elsewhere in this guide.");

    expect(buildEmptyStateMessage({
      activeAreaLabel: "South Brisbane",
      overflowCount: 1,
      query: "coffee",
    })).toBe('No places matched "coffee" in South Brisbane. 1 more match elsewhere in this guide. Try another area or clear the area filter.');
  });

  it("suppresses the area status line when no places are visible in the selected area", () => {
    expect(buildAreaFilterStatusMessage({
      activeAreaLabel: "South Brisbane",
      visibleCount: 0,
      overflowCount: 2,
    })).toBe("");
  });

  it("keeps the area label in query empty-state copy even with no overflow matches", () => {
    expect(buildEmptyStateMessage({
      activeAreaLabel: "South Brisbane",
      overflowCount: 0,
      query: "coffee",
    })).toBe('No places matched "coffee" in South Brisbane. Try another area or clear the area filter.');
  });

  it("lets guide-wide overflow ignore the current map frame", () => {
    const cards = [
      makeCard({ placeId: "1", neighborhood: "south brisbane", search: "brunch", vibeTags: "date-night" }),
      makeCard({ placeId: "2", neighborhood: "west end", search: "brunch", vibeTags: "date-night" }),
    ];

    expect(countMatchingCards(cards, {
      activeArea: "south brisbane",
      mapFramePlaceIds: new Set(["1"]),
      normalizedQuery: "brunch",
      searchResultIds: new Set(["1", "2"]),
      tag: "date-night",
    })).toBe(1);

    expect(countMatchingCards(cards, {
      normalizedQuery: "brunch",
      searchResultIds: new Set(["1", "2"]),
      tag: "date-night",
    })).toBe(2);
  });
});
