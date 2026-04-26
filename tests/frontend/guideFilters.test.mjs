import { describe, expect, it } from "vitest";

import {
  buildAreaFilterStatusMessage,
  buildEmptyStateMessage,
  buildNearbyDistanceMap,
  cardHasTag,
  cardMatchesType,
  compareCardsByCurated,
  compareCardsByNearby,
  countAreaOptionCards,
  countMatchingCards,
  countTagOptionCards,
  countTypeOptionCards,
  resolveLocationSortState,
  sortFilterOptions,
} from "../../public/scripts/guide-filters.js";

const makeCard = ({
  bestHit = "false",
  category = "",
  featured = "false",
  lat = "",
  lng = "",
  localityPath = "",
  name = "",
  neighborhood = "",
  placeId,
  rank = "0",
  search = "",
  tags = "",
  topPick = "false",
  vibeTags = "",
} = {}) => ({
  dataset: {
    bestHit,
    category,
    featured,
    lat,
    lng,
    localityPath,
    name,
    neighborhood,
    placeId,
    rank,
    search,
    tags,
    topPick,
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
        hasAdditionalFilters: true,
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
        hasAdditionalFilters: true,
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
        hasAdditionalFilters: true,
        visibleCount: 0,
        overflowCount: 2,
      }),
    ).toBe("");
  });

  it("suppresses the area status line when area is the only active filter", () => {
    expect(
      buildAreaFilterStatusMessage({
        activeAreaLabel: "Zhongshan",
        hasAdditionalFilters: false,
        visibleCount: 3,
        overflowCount: 8,
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

  it("matches broader-area filters against any locality in the card path", () => {
    const cards = [
      makeCard({
        placeId: "1",
        neighborhood: "Jingumae",
        localityPath: JSON.stringify(["Jingumae", "Shibuya City"]),
        search: "coffee",
      }),
      makeCard({
        placeId: "2",
        neighborhood: "Nishiazabu",
        localityPath: JSON.stringify(["Nishiazabu", "Minato City"]),
        search: "coffee",
      }),
    ];

    expect(
      countMatchingCards(cards, {
        activeArea: "broader-shibuya",
        normalizedQuery: "coffee",
      }),
    ).toBe(1);
  });

  it("matches primary areas across district suffix variants without collapsing broader levels", () => {
    const cards = [
      makeCard({
        placeId: "1",
        neighborhood: "Zhongshan District",
        localityPath: JSON.stringify(["Zhongshan District"]),
        search: "dumplings",
      }),
      makeCard({
        placeId: "2",
        neighborhood: "Meguro",
        localityPath: JSON.stringify(["Meguro", "Meguro City"]),
        search: "coffee",
      }),
    ];

    expect(
      countMatchingCards(cards, {
        activeArea: "zhongshan",
        normalizedQuery: "dumplings",
      }),
    ).toBe(1);

    expect(
      countMatchingCards(cards, {
        activeArea: "broader-meguro",
        normalizedQuery: "coffee",
      }),
    ).toBe(1);

    expect(
      countMatchingCards(cards, {
        activeArea: "meguro",
        normalizedQuery: "coffee",
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

  it("keeps the original order within equal-count groups even when one option is active", () => {
    expect(
      sortFilterOptions([
        { pinned: true, active: false, count: 11, originalIndex: 0, id: "all" },
        { pinned: false, active: false, count: 2, originalIndex: 1, id: "da-an" },
        { pinned: false, active: false, count: 2, originalIndex: 2, id: "taipei-city" },
        { pinned: false, active: false, count: 2, originalIndex: 3, id: "zhongshan" },
        { pinned: false, active: true, count: 1, originalIndex: 4, id: "datong" },
        { pinned: false, active: false, count: 1, originalIndex: 5, id: "shilin" },
        { pinned: false, active: false, count: 1, originalIndex: 6, id: "wenshan" },
      ]).map((option) => option.id),
    ).toEqual(["all", "da-an", "taipei-city", "zhongshan", "datong", "shilin", "wenshan"]);
  });

  it("sorts nearby cards by cached distance and keeps cards without coordinates last", () => {
    const currentLocation = { lat: 35.6812, lng: 139.7671 };
    const cards = [
      makeCard({ placeId: "far", lat: "35.6895", lng: "139.6917", name: "Far", rank: "1" }),
      makeCard({ placeId: "missing", name: "Missing", rank: "99", topPick: "true" }),
      makeCard({ placeId: "near", lat: "35.6814", lng: "139.7673", name: "Near", rank: "2" }),
    ];
    const distanceByPlaceId = buildNearbyDistanceMap(cards, currentLocation);

    const sortedCards = [...cards].sort((left, right) =>
      compareCardsByNearby(left, right, {
        currentLocation,
        distanceByPlaceId,
      }),
    );

    expect(Array.from(distanceByPlaceId.keys())).toEqual(["far", "near"]);
    expect(sortedCards.map((card) => card.dataset.placeId)).toEqual(["near", "far", "missing"]);
  });

  it("falls back to curated sorting when nearby sorting has no current location", () => {
    const cards = [
      makeCard({ placeId: "rank-1", name: "Bravo", rank: "1" }),
      makeCard({ placeId: "rank-3", name: "Alpha", rank: "3" }),
      makeCard({ placeId: "top-pick", name: "Cafe", rank: "2", topPick: "true" }),
    ];

    const nearbySorted = [...cards].sort((left, right) =>
      compareCardsByNearby(left, right, {
        currentLocation: null,
        distanceByPlaceId: new Map(),
      }),
    );
    const curatedSorted = [...cards].sort(compareCardsByCurated);

    expect(nearbySorted.map((card) => card.dataset.placeId)).toEqual(
      curatedSorted.map((card) => card.dataset.placeId),
    );
  });

  it("prioritizes featured and best-hit cards in curated sorting", () => {
    const cards = [
      makeCard({ placeId: "rank-10", name: "Bravo", rank: "10" }),
      makeCard({ placeId: "best-hit", name: "Alpha", rank: "1", bestHit: "true" }),
      makeCard({ placeId: "top-pick", name: "Cafe", rank: "99", topPick: "true" }),
      makeCard({ placeId: "featured", name: "Delta", rank: "0", featured: "true" }),
    ];

    expect([...cards].sort(compareCardsByCurated).map((card) => card.dataset.placeId)).toEqual([
      "featured",
      "best-hit",
      "top-pick",
      "rank-10",
    ]);
  });

  it("resets nearby sorting to curated when location is denied or unavailable", () => {
    expect(
      resolveLocationSortState({
        currentLocation: null,
        currentLocationStatus: "denied",
        sortValue: "nearby",
      }),
    ).toEqual({
      message: "Location unavailable. Showing curated order instead.",
      shouldFallback: true,
      sortValue: "curated",
    });

    expect(
      resolveLocationSortState({
        currentLocation: null,
        currentLocationStatus: "unavailable",
        fallbackMessage: "Location denied.",
        fallbackSortValue: "rating",
        sortValue: "nearby",
      }),
    ).toEqual({
      message: "Location denied.",
      shouldFallback: true,
      sortValue: "rating",
    });
  });

  it("does not reset nearby sorting while location is still idle, checking, or already available", () => {
    expect(
      resolveLocationSortState({
        currentLocation: null,
        currentLocationStatus: "idle",
        sortValue: "nearby",
      }),
    ).toEqual({
      message: "",
      shouldFallback: false,
      sortValue: "nearby",
    });

    expect(
      resolveLocationSortState({
        currentLocation: null,
        currentLocationStatus: "checking",
        sortValue: "nearby",
      }),
    ).toEqual({
      message: "",
      shouldFallback: false,
      sortValue: "nearby",
    });

    expect(
      resolveLocationSortState({
        currentLocation: { lat: 35.6812, lng: 139.7671 },
        currentLocationStatus: "available",
        sortValue: "nearby",
      }),
    ).toEqual({
      message: "",
      shouldFallback: false,
      sortValue: "nearby",
    });
  });
});
