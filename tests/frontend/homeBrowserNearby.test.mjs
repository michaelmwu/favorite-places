import { describe, expect, it } from "vitest";

import {
  nearbyGuideConfig,
  nearbyGuidesForLocation,
} from "../../public/scripts/home-browser-nearby.js";

const buildGuide = (slug, lat, lng) => ({
  slug,
  title: slug,
  country: "testland",
  countryName: "Testland",
  lat,
  lng,
});

describe("home browser nearby guide selection", () => {
  it("returns null when no guide locations are available", () => {
    expect(nearbyGuidesForLocation([], 35.68, 139.76)).toBeNull();
  });

  it("always includes the nearest guide even beyond the max radius cap", () => {
    const farGuide = buildGuide("far-away", 10, 10);

    const result = nearbyGuidesForLocation([farGuide], 35.68, 139.76);

    expect(result?.guides.map((guide) => guide.slug)).toEqual(["far-away"]);
    expect(result?.guideSlugs.has("far-away")).toBe(true);
    expect(result?.radiusKm).toBeCloseTo(result?.nearestGuide.distance ?? 0, 6);
    expect(result?.nearestGuide.distance ?? 0).toBeGreaterThan(
      nearbyGuideConfig.MAX_NEARBY_RADIUS_KM,
    );
  });

  it("caps the nearby list to the configured maximum guide count", () => {
    const guides = Array.from({ length: 12 }, (_, index) =>
      buildGuide(`guide-${index + 1}`, 35 + index * 0.1, 139),
    );

    const result = nearbyGuidesForLocation(guides, 35, 139);

    expect(result?.guides).toHaveLength(nearbyGuideConfig.MAX_NEARBY_GUIDES);
    expect(result?.guides[0]?.slug).toBe("guide-1");
    expect(result?.guides.at(-1)?.slug).toBe("guide-8");
  });

  it("builds a guide slug set that matches the returned nearby guides", () => {
    const guides = [
      buildGuide("tokyo", 35.68, 139.76),
      buildGuide("osaka", 34.69, 135.5),
      buildGuide("seoul", 37.56, 126.97),
      buildGuide("sydney", -33.86, 151.2),
    ];

    const result = nearbyGuidesForLocation(guides, 35.68, 139.76);

    expect([...result.guideSlugs]).toEqual(result.guides.map((guide) => guide.slug));
    expect(result.guideSlugs.has("tokyo")).toBe(true);
    expect(result.guideSlugs.has("sydney")).toBe(false);
  });
});
