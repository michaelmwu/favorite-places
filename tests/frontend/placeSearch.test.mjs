import { describe, expect, it } from "vitest";

import { prepareSearchIndex, searchGuides, searchPlaces } from "../../public/scripts/place-search.js";

const index = prepareSearchIndex({
  version: 1,
  guides: [
    {
      slug: "tokyo-japan",
      title: "Tokyo, Japan",
      city: "Tokyo",
      country: "Japan",
      country_code: "JP",
      tags: ["coffee"],
      place_count: 2,
      top_categories: ["Coffee shop"],
      featured_names: ["Quiet Coffee"],
      url: "/guides/tokyo-japan/",
      search_text: "tokyo japan coffee",
    },
    {
      slug: "san-francisco-california-usa",
      title: "San Francisco, California, USA",
      city: "San Francisco",
      country: "United States",
      country_code: "US",
      tags: ["restaurants"],
      place_count: 1,
      top_categories: ["Restaurant"],
      featured_names: ["Buzzy Dinner"],
      url: "/guides/san-francisco-california-usa/",
      search_text: "san francisco california sf restaurants",
    },
  ],
  entries: [
    {
      id: "tokyo-coffee",
      guide_slug: "tokyo-japan",
      guide_title: "Tokyo, Japan",
      city: "Tokyo",
      country: "Japan",
      name: "Quiet Coffee",
      category: "Coffee shop",
      neighborhood: "Shibuya",
      tags: ["coffee-shop", "shibuya"],
      vibe_tags: ["quiet", "laptop-friendly", "cozy"],
      note: "Quiet coffee shop with wifi.",
      top_pick: true,
      manual_rank: 2,
      maps_url: "https://maps.example/tokyo-coffee",
      url: "/guides/tokyo-japan/?place=tokyo-coffee",
      search_text: "quiet coffee shop wifi shibuya tokyo japan",
    },
    {
      id: "sf-dinner",
      guide_slug: "san-francisco-california-usa",
      guide_title: "San Francisco, California, USA",
      city: "San Francisco",
      country: "United States",
      name: "Buzzy Dinner",
      category: "Restaurant",
      neighborhood: "Mission",
      tags: ["restaurant", "mission"],
      vibe_tags: ["date-night", "lively"],
      note: "Date night restaurant.",
      top_pick: false,
      manual_rank: 0,
      maps_url: "https://maps.example/sf-dinner",
      url: "/guides/san-francisco-california-usa/?place=sf-dinner",
      search_text: "date night restaurant mission san francisco sf",
    },
  ],
});

describe("place search", () => {
  it("parses natural global queries into location, category, and vibe constraints", () => {
    const state = searchPlaces("quiet coffee in tokyo", { index, scope: "all" });

    expect(state.results.map((result) => result.entry.id)).toEqual(["tokyo-coffee"]);
    expect(state.parsed.guideSlugs).toEqual(["tokyo-japan"]);
    expect(state.parsed.categories).toEqual(["cafe"]);
    expect(state.parsed.vibes).toContain("quiet");
  });

  it("keeps explicit location matches above unrelated cities", () => {
    const state = searchPlaces("date night restaurants in sf", { index, scope: "all" });

    expect(state.results[0].entry.id).toBe("sf-dinner");
    expect(state.results.every((result) => result.entry.guide_slug === "san-francisco-california-usa")).toBe(true);
  });

  it("supports guide-local queries without repeating the city name", () => {
    const state = searchPlaces("laptop friendly coffee", {
      index,
      scope: "guide",
      guideSlug: "tokyo-japan",
    });

    expect(state.results.map((result) => result.entry.id)).toEqual(["tokyo-coffee"]);
  });

  it("returns guide matches for broad home-page discovery", () => {
    expect(searchGuides("sf restaurants", { index })[0].guide.slug).toBe("san-francisco-california-usa");
  });
});
