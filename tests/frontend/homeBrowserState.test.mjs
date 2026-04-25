import { describe, expect, it } from "vitest";

import {
  parseHomeBrowserHash,
  serializeHomeBrowserHash,
  slugifyCountry,
} from "../../public/scripts/home-browser-state.js";

describe("home browser URL state", () => {
  const validCountries = ["japan", "united states", "south korea"];

  it("serializes country-only filters as compact hashtags", () => {
    expect(serializeHomeBrowserHash({ country: "Japan" })).toBe("#japan");
    expect(serializeHomeBrowserHash({ country: "United States" })).toBe("#united-states");
  });

  it("serializes search text and result view in query-style hashes", () => {
    expect(
      serializeHomeBrowserHash({
        country: "Japan",
        query: "quiet coffee",
        view: "individual",
      }),
    ).toBe("#country=japan&q=quiet+coffee&view=individual");
  });

  it("parses compact country hashtags back to known countries", () => {
    expect(parseHomeBrowserHash("#united-states", validCountries)).toEqual({
      country: "united states",
      query: "",
      view: "grouped",
    });
  });

  it("parses query-style hashes for country, search text, and view", () => {
    expect(
      parseHomeBrowserHash("#country=south-korea&q=date+night&view=individual", validCountries),
    ).toEqual({
      country: "south korea",
      query: "date night",
      view: "individual",
    });
  });

  it("ignores unknown countries while keeping other shareable state", () => {
    expect(parseHomeBrowserHash("#country=mars&q=quiet", validCountries)).toEqual({
      country: "",
      query: "quiet",
      view: "grouped",
    });
  });

  it("slugifies country labels consistently", () => {
    expect(slugifyCountry("United States")).toBe("united-states");
    expect(slugifyCountry("South Korea")).toBe("south-korea");
  });
});
