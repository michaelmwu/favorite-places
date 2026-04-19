import { describe, expect, it } from "vitest";

import {
  buildMapMarkerSvg,
  getMapMarkerColors,
  getMapMarkerLabel,
  getMapMarkerSize,
} from "../../src/lib/mapMarkerIcons";

describe("map marker icons", () => {
  it("renders distinct SVG markers for place types", () => {
    const cafeSvg = buildMapMarkerSvg("cafe", getMapMarkerColors("cafe"));
    const museumSvg = buildMapMarkerSvg("museum", getMapMarkerColors("museum"));

    expect(cafeSvg).toContain("<title>Cafe</title>");
    expect(museumSvg).toContain("<title>Museum</title>");
    expect(cafeSvg).not.toEqual(museumSvg);
  });

  it("labels the generic fallback marker clearly", () => {
    expect(getMapMarkerLabel("default")).toBe("Saved place");
  });

  it("scales active top picks larger than the default marker", () => {
    const baseSize = getMapMarkerSize("restaurant", false, false);
    const activeTopPickSize = getMapMarkerSize("restaurant", true, true);

    expect(activeTopPickSize.width).toBeGreaterThan(baseSize.width);
    expect(activeTopPickSize.height).toBeGreaterThan(baseSize.height);
    expect(activeTopPickSize.anchorY).toBeGreaterThan(baseSize.anchorY);
  });

  it("keeps the default marker on the same circular badge geometry", () => {
    const defaultSize = getMapMarkerSize("default", false, false);
    const restaurantSize = getMapMarkerSize("restaurant", false, false);
    const defaultSvg = buildMapMarkerSvg("default", getMapMarkerColors("default"));

    expect(defaultSize.height).toBe(restaurantSize.height);
    expect(defaultSize.anchorY).toBe(restaurantSize.anchorY);
    expect(defaultSvg).toContain('viewBox="0 0 40 40"');
  });

  it("uses distinct type palettes and adds a ring for active or top-pick markers", () => {
    const cafeColors = getMapMarkerColors("cafe");
    const parkColors = getMapMarkerColors("park", { topPick: true });
    const activeRestaurantColors = getMapMarkerColors("restaurant", { active: true });

    expect(cafeColors.fillColor).not.toBe(parkColors.fillColor);
    expect(parkColors.ringColor).toBeTruthy();
    expect(activeRestaurantColors.ringColor).toBeTruthy();
  });
});
