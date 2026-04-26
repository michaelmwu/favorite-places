import { describe, expect, it } from "vitest";

import {
  getDisplayGuideTags,
  getDisplayPlaceTags,
  getGuideAreaFilters,
  getTagComparisonValue,
  normalizeTagValue,
} from "../../src/lib/placeTags";

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
      { label: "Juárez", value: "juarez", count: 3 },
      { label: "Shibuya", value: "shibuya", count: 3 },
    ]);
  });

  it("backfills with valid one-off areas when repeated neighborhoods are sparse", () => {
    expect(
      getGuideAreaFilters(
        [
          { neighborhood: "Ciutat Vella" },
          { neighborhood: "Ciutat Vella" },
          { neighborhood: "Ciutat Vella" },
          { neighborhood: "Eixample" },
          { neighborhood: "Eixample" },
          { neighborhood: "Eixample" },
          { neighborhood: "Gràcia" },
          { neighborhood: "Port Vell" },
          { neighborhood: "Les Corts" },
          { neighborhood: "C/ d'Aribau" },
          { neighborhood: "Ctra. de Miramar" },
        ],
        { limit: 5 },
      ),
    ).toEqual([
      { label: "Ciutat Vella", value: "ciutat-vella", count: 3 },
      { label: "Eixample", value: "eixample", count: 3 },
      { label: "Gràcia", value: "gracia", count: 1 },
      { label: "Les Corts", value: "les-corts", count: 1 },
      { label: "Port Vell", value: "port-vell", count: 1 },
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

  it("groups area filters by normalized text so variant spellings share one pill", () => {
    expect(
      getGuideAreaFilters([
        { neighborhood: "São Paulo" },
        { neighborhood: "Sao Paulo" },
        { neighborhood: "São Paulo" },
        { neighborhood: "Pinheiros" },
        { neighborhood: "Pinheiros" },
      ]),
    ).toEqual([
      { label: "São Paulo", value: "sao-paulo", count: 3 },
      { label: "Pinheiros", value: "pinheiros", count: 2 },
    ]);
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

  it("hides translated city aliases for display while keeping them searchable", () => {
    expect(
      getDisplayGuideTags(["geneve", "geneva", "park"], {
        cityName: "Genève",
        countryCode: "CH",
        countryName: "Switzerland",
      }),
    ).toEqual(["park"]);
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

  it("keeps non-latin tags when they are not location duplicates", () => {
    expect(
      getDisplayGuideTags(["東京", "coffee"], {
        cityName: "Tonga",
        countryCode: "TO",
        countryName: "Tonga",
      }),
    ).toEqual(["東京", "coffee"]);
  });
});

describe("normalizeTagValue", () => {
  it("slugifies human-readable vibe overrides for guide filter matching", () => {
    expect(normalizeTagValue("Date Night")).toBe("date-night");
    expect(normalizeTagValue("Laptop Friendly")).toBe("laptop-friendly");
    expect(normalizeTagValue("Genève")).toBe("geneve");
  });
});

describe("getTagComparisonValue", () => {
  it("falls back to normalized non-latin text when ascii slugification is empty", () => {
    expect(getTagComparisonValue("東京")).toBe("東京");
  });
});
