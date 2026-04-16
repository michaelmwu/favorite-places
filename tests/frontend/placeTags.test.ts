import { describe, expect, it } from "vitest";

import { getDisplayGuideTags, getDisplayPlaceTags, getGuideAreaFilters } from "../../src/lib/placeTags";

describe("getDisplayPlaceTags", () => {
  it("keeps useful tags and hides address fragments", () => {
    expect(
      getDisplayPlaceTags([
        "coffee",
        "jingumae",
        "shibuya-city",
        "district-1",
        "3rd-wave",
        "street-food",
        "st-kilda",
        "tokyo-150-0001",
        "chome-17-5-okubo",
        "best-select-bldg",
        "ginza-777-adc-building",
        "gicros-ginza-gems",
        "le-gratteciel",
      ]),
    ).toEqual([
      "coffee",
      "jingumae",
      "shibuya-city",
      "district-1",
      "3rd-wave",
      "street-food",
      "st-kilda",
    ]);
  });
});

describe("getGuideAreaFilters", () => {
  it("keeps repeated area filters and drops one-off or street-like labels", () => {
    expect(
      getGuideAreaFilters(
        [
          { neighborhood: "Ginza" },
          { neighborhood: "Ginza" },
          { neighborhood: "Ginza" },
          { neighborhood: "Shibuya" },
          { neighborhood: "Shibuya" },
          { neighborhood: "Shibuya" },
          { neighborhood: "Juárez" },
          { neighborhood: "Juárez" },
          { neighborhood: "Juárez" },
          { neighborhood: "C/ d'Aribau" },
          { neighborhood: "C/ d'Aribau" },
          { neighborhood: "C/ d'Aribau" },
          { neighborhood: "One-off" },
        ],
        { limit: 3 },
      ),
    ).toEqual([
      { label: "Ginza", value: "ginza", count: 3 },
      { label: "Juárez", value: "juárez", count: 3 },
      { label: "Shibuya", value: "shibuya", count: 3 },
    ]);
  });

  it("drops no-op areas that would match every place", () => {
    expect(
      getGuideAreaFilters([
        { neighborhood: "Tokyo" },
        { neighborhood: "Tokyo" },
        { neighborhood: "Tokyo" },
      ]),
    ).toEqual([]);
  });
});

describe("getDisplayGuideTags", () => {
  it("hides broad city and country tags while keeping more specific local tags", () => {
    expect(
      getDisplayGuideTags(
        [
          "brisbane",
          "gold-coast",
          "brisbane-gold-coast",
          "australia",
          "south-brisbane",
          "brisbane-city-hall",
          "coffee",
        ],
        {
          cityName: "Brisbane & Gold Coast",
          countryCode: "AU",
          countryName: "Australia",
        },
      ),
    ).toEqual(["south-brisbane", "brisbane-city-hall", "coffee"]);
  });

  it("drops common country aliases for display", () => {
    expect(
      getDisplayGuideTags(["usa", "new-york", "pizza"], {
        cityName: "New York",
        countryCode: "US",
        countryName: "United States",
      }),
    ).toEqual(["pizza"]);
  });

  it("drops single-location country tags such as tonga", () => {
    expect(
      getDisplayGuideTags(["tonga"], {
        cityName: "Tonga",
        countryCode: "TO",
        countryName: "Tonga",
      }),
    ).toEqual([]);
  });
});
