import { describe, expect, it } from "vitest";

import {
  buildAreaFilterStatusMessage,
  buildEmptyStateMessage,
  cardHasTag,
  cardMatchesType,
  countAreaOptionCards,
  countMatchingCards,
  countTagOptionCards,
  countTypeOptionCards,
  sortFilterOptions,
} from "../../public/scripts/guide-filters.js";

const makeCard = ({
  category = "",
  neighborhood = "",
  placeId,
  search = "",
  tags = "",
  vibeTags = "",
}) => ({
  dataset: {
    category,
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
      makeCard({
        placeId: "1",
        neighborhood: "south brisbane",
        search: "coffee brunch",
        vibeTags: "date-night scenic",
      }),
      makeCard({
        placeId: "2",
        neighborhood: "south brisbane",
        search: "rain museum",
        vibeTags: "rainy-day",
      }),
      makeCard({
        placeId: "3",
        neighborhood: "west end",
        search: "sunset drinks",
        vibeTags: "date-night scenic",
      }),
    ];

    expect(
      countMatchingCards(cards, {
        activeArea: "south brisbane",
        normalizedQuery: "",
        tag: "date-night",
      }),
    ).toBe(1);
    expect(
      countMatchingCards(cards, {
        activeArea: "south brisbane",
        normalizedQuery: "",
        tag: "",
      }),
    ).toBe(2);
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

  it("normalizes multi-word categories before type matching", () => {
    const card = makeCard({ placeId: "1" });
    card.dataset.category = "Apartment Building";

    expect(
      cardMatchesType(card, {
        activeTypeValue: "apartment-building",
        activeTypeSeedValues: [],
      }),
    ).toBe(true);
  });

  it("builds area-aware status and empty messages when broader matches exist", () => {
    expect(
      buildAreaFilterStatusMessage({
        activeAreaLabel: "South Brisbane",
        visibleCount: 1,
        overflowCount: 3,
      }),
    ).toBe("Showing 1 place in South Brisbane. 3 more matches elsewhere in this guide.");

    expect(
      buildEmptyStateMessage({
        activeAreaLabel: "South Brisbane",
        overflowCount: 2,
        query: "",
      }),
    ).toBe(
      "No matches in South Brisbane. 2 more matches elsewhere in this guide. Try another area or clear the area filter.",
    );
  });

  it("keeps generic empty-state copy when no area filter is active", () => {
    expect(
      buildEmptyStateMessage({
        activeAreaLabel: "",
        overflowCount: 2,
        query: "",
      }),
    ).toBe("No matches. Try a broader search or clear the tag filter.");
  });

  it("uses singular overflow grammar for a single broader result", () => {
    expect(
      buildAreaFilterStatusMessage({
        activeAreaLabel: "South Brisbane",
        visibleCount: 1,
        overflowCount: 1,
      }),
    ).toBe("Showing 1 place in South Brisbane. 1 more match elsewhere in this guide.");

    expect(
      buildEmptyStateMessage({
        activeAreaLabel: "South Brisbane",
        overflowCount: 1,
        query: "coffee",
      }),
    ).toBe(
      'No places matched "coffee" in South Brisbane. 1 more match elsewhere in this guide. Try another area or clear the area filter.',
    );
  });

  it("suppresses the area status line when no places are visible in the selected area", () => {
    expect(
      buildAreaFilterStatusMessage({
        activeAreaLabel: "South Brisbane",
        visibleCount: 0,
        overflowCount: 2,
      }),
    ).toBe("");
  });

  it("keeps the area label in query empty-state copy even with no overflow matches", () => {
    expect(
      buildEmptyStateMessage({
        activeAreaLabel: "South Brisbane",
        overflowCount: 0,
        query: "coffee",
      }),
    ).toBe(
      'No places matched "coffee" in South Brisbane. Try another area or clear the area filter.',
    );
  });

  it("lets guide-wide overflow ignore the current map frame", () => {
    const cards = [
      makeCard({
        placeId: "1",
        neighborhood: "south brisbane",
        search: "brunch",
        vibeTags: "date-night",
      }),
      makeCard({
        placeId: "2",
        neighborhood: "west end",
        search: "brunch",
        vibeTags: "date-night",
      }),
    ];

    expect(
      countMatchingCards(cards, {
        activeArea: "south brisbane",
        mapFramePlaceIds: new Set(["1"]),
        normalizedQuery: "brunch",
        searchResultIds: new Set(["1", "2"]),
        tag: "date-night",
      }),
    ).toBe(1);

    expect(
      countMatchingCards(cards, {
        normalizedQuery: "brunch",
        searchResultIds: new Set(["1", "2"]),
        tag: "date-night",
      }),
    ).toBe(2);
  });

  it("does not count off-screen matches in the same area as guide-wide overflow", () => {
    const cards = [
      makeCard({
        placeId: "1",
        neighborhood: "south brisbane",
        search: "brunch",
        vibeTags: "date-night",
      }),
      makeCard({
        placeId: "2",
        neighborhood: "south brisbane",
        search: "brunch",
        vibeTags: "date-night",
      }),
    ];

    const broaderAreaCount = countMatchingCards(cards, {
      normalizedQuery: "brunch",
      searchResultIds: new Set(["1", "2"]),
      tag: "date-night",
    });
    const areaMatchCount = countMatchingCards(cards, {
      activeArea: "south brisbane",
      normalizedQuery: "brunch",
      searchResultIds: new Set(["1", "2"]),
      tag: "date-night",
    });
    const visibleCountInMapFrame = countMatchingCards(cards, {
      activeArea: "south brisbane",
      mapFramePlaceIds: new Set(["1"]),
      normalizedQuery: "brunch",
      searchResultIds: new Set(["1", "2"]),
      tag: "date-night",
    });

    expect(broaderAreaCount).toBe(2);
    expect(areaMatchCount).toBe(2);
    expect(visibleCountInMapFrame).toBe(1);
    expect(Math.max(0, broaderAreaCount - areaMatchCount)).toBe(0);
  });

  it("normalizes accented and slugged area filters before matching cards", () => {
    const cards = [
      makeCard({
        placeId: "1",
        neighborhood: "São Paulo",
        search: "coffee",
        vibeTags: "date-night",
      }),
      makeCard({
        placeId: "2",
        neighborhood: "Rio de Janeiro",
        search: "coffee",
        vibeTags: "date-night",
      }),
    ];

    expect(
      countMatchingCards(cards, {
        activeArea: "sao-paulo",
        normalizedQuery: "coffee",
        tag: "date-night",
      }),
    ).toBe(1);
  });

  it("counts area options against the current non-area filters", () => {
    const cards = [
      makeCard({
        placeId: "1",
        neighborhood: "lastarria",
        category: "cafe",
        vibeTags: JSON.stringify(["cozy"]),
      }),
      makeCard({
        placeId: "2",
        neighborhood: "providencia",
        category: "restaurant",
        vibeTags: JSON.stringify(["cozy"]),
      }),
      makeCard({
        placeId: "3",
        neighborhood: "bellavista",
        category: "cafe",
        vibeTags: JSON.stringify(["lively"]),
      }),
    ];

    expect(
      countAreaOptionCards(
        cards,
        {
          activeTypeValue: "cafe",
          activeTypeSeedValues: [],
          selectedTagValues: ["cozy"],
        },
        "lastarria",
      ),
    ).toBe(1);
    expect(
      countAreaOptionCards(cards, {
        activeTypeValue: "cafe",
        activeTypeSeedValues: [],
        selectedTagValues: ["cozy"],
      }),
    ).toBe(1);
  });

  it("counts type and tag options within the selected area", () => {
    const cards = [
      makeCard({
        placeId: "1",
        neighborhood: "lastarria",
        category: "cafe",
        vibeTags: JSON.stringify(["cozy"]),
      }),
      makeCard({
        placeId: "2",
        neighborhood: "lastarria",
        category: "restaurant",
        vibeTags: JSON.stringify(["date-night"]),
      }),
      makeCard({
        placeId: "3",
        neighborhood: "providencia",
        category: "restaurant",
        vibeTags: JSON.stringify(["date-night"]),
      }),
    ];

    expect(
      countTypeOptionCards(
        cards,
        {
          activeArea: "lastarria",
        },
        {
          typeValue: "restaurant",
          typeSeedValues: [],
        },
      ),
    ).toBe(1);
    expect(
      countTagOptionCards(
        cards,
        {
          activeArea: "lastarria",
          activeTypeValue: "restaurant",
          activeTypeSeedValues: [],
        },
        "cozy",
      ),
    ).toBe(0);
  });

  it("sorts matching filter options ahead of zero-count options while keeping pinned items first", () => {
    expect(
      sortFilterOptions([
        { pinned: false, active: false, count: 0, originalIndex: 3, id: "zero" },
        { pinned: false, active: false, count: 2, originalIndex: 2, id: "two" },
        { pinned: true, active: false, count: 1, originalIndex: 0, id: "all" },
        { pinned: false, active: false, count: 5, originalIndex: 1, id: "five" },
      ]).map((option) => option.id),
    ).toEqual(["all", "five", "two", "zero"]);
  });
});
