import { describe, expect, it } from "vitest";

import { getDisplayPlaceTags } from "../../src/lib/placeTags";

describe("getDisplayPlaceTags", () => {
  it("keeps useful tags and hides address fragments", () => {
    expect(
      getDisplayPlaceTags([
        "coffee",
        "jingumae",
        "shibuya-city",
        "tokyo-150-0001",
        "chome-17-5-okubo",
        "best-select-bldg",
        "ginza-777-adc-building",
        "gicros-ginza-gems",
        "le-gratteciel",
      ]),
    ).toEqual(["coffee", "jingumae", "shibuya-city"]);
  });
});
