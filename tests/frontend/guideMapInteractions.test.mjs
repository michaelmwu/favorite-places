import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const readSource = (path) => readFileSync(path, "utf8");
const cssBlocks = (css, selector) => {
  return [...css.matchAll(/([^{}]+){([^}]*)}/g)]
    .filter((match) => match[1].split(",").map((part) => part.trim()).includes(selector))
    .map((match) => match[2]);
};

describe("guide map interactions", () => {
  it("keeps guide search controls in normal document flow while the map remains sticky", () => {
    const css = readSource("src/styles/global.css");

    expect(cssBlocks(css, ".controls-panel").join("\n")).not.toContain("position: sticky");
    expect(cssBlocks(css, ".country-browser").join("\n")).toContain("position: sticky");
    expect(cssBlocks(css, ".map-panel").join("\n")).toContain("position: sticky");
  });

  it("lets the collapsed map panel release width back to the places list", () => {
    const guidePage = readSource("src/pages/guides/[slug].astro");
    const guideMap = readSource("src/components/GuideMap.astro");
    const css = readSource("src/styles/global.css");

    expect(guidePage).toContain('class="browse-layout" data-browse-layout');
    expect(guidePage).toContain('class="browse-main" data-browse-main');
    expect(guidePage.indexOf('<div class="browse-main" data-browse-main>')).toBeLessThan(
      guidePage.indexOf("<GuideMap places={visiblePlaces} />"),
    );
    expect(guideMap).toContain('const DESKTOP_MAP_MEDIA_QUERY = "(min-width: 980px)"');
    expect(guideMap).toContain('const syncPanelOrder = (panel: HTMLElement) => {');
    expect(guideMap).toContain('const browseMain = layout?.querySelector<HTMLElement>("[data-browse-main]")');
    expect(guideMap).toContain("browseMain.after(panel);");
    expect(guideMap).toContain("layout.prepend(panel);");
    expect(guideMap).toContain('panel.closest<HTMLElement>("[data-browse-layout]")');
    expect(guideMap).toContain("layout.dataset.mapCollapsed");
    expect(css).toContain('.browse-layout[data-map-collapsed="true"]');
    expect(css).toContain("grid-template-columns: minmax(0, 1fr) 2.75rem");
    expect(cssBlocks(css, ".map-panel").join("\n")).toContain("order: 0;");
    expect(css).toContain('.map-panel[data-collapsed="true"] .map-toggle');
    expect(css).toContain("border-color: transparent");
    expect(css).toContain("background: transparent");
    expect(css).not.toContain("inset: 0;\n  width: 100%;\n  height: 100%;");
  });

  it("keeps map-frame filtering resettable from both the map and the list", () => {
    const guidePage = readSource("src/pages/guides/[slug].astro");
    const guideMap = readSource("src/components/GuideMap.astro");
    const filters = readSource("public/scripts/guide-filters.js");

    expect(guidePage).toContain("data-area-filter-status");
    expect(guidePage).toContain("data-map-filter-status");
    expect(guidePage).toContain("data-tag-count-text");
    expect(guidePage).toContain("data-map-filter-reset");
    expect(guideMap).toContain("data-map-frame-filter");
    expect(guideMap).toContain("data-map-full-area");
    expect(guideMap).toContain(">Reset Map</button>");
    expect(guideMap).toContain('new CustomEvent("guide:map-frame-reset-request"');
    expect(guideMap).toContain('new CustomEvent("guide:map-frame-filter"');
    expect(filters).toContain("countMatchingCards");
    expect(filters).toContain("buildAreaFilterStatusMessage");
    expect(filters).toContain('root.addEventListener("guide:map-frame-filter"');
    expect(filters).toContain('root.dispatchEvent(new CustomEvent("guide:map-frame-reset"');
  });

  it("guards the current-location map control with stored guide proximity bounds", () => {
    const guideMap = readSource("src/components/GuideMap.astro");
    const css = readSource("src/styles/global.css");

    expect(guideMap).toContain('class="map-icon-button"');
    expect(guideMap).toContain('data-location-state="idle"');
    expect(guideMap).toContain('aria-label="Use current location"');
    expect(guideMap).toContain('aria-disabled="false"');
    expect(guideMap).not.toContain(">Near me</button>");
    expect(guideMap).toContain("locationBoundsForPlaces");
    expect(guideMap).toContain("guideLocationInliers");
    expect(guideMap).toContain("guideLocationBounds");
    expect(guideMap).toContain("(sorted.length - 1) * percentileValue");
    expect(guideMap).not.toContain("Math.ceil(sorted.length * percentileValue) - 1");
    expect(guideMap).toContain("distanceFromGuideCenter <= guideLocationBounds.maxDistanceKm");
    expect(guideMap).toContain("Current location is too far from this guide");
    expect(guideMap).toContain('setLocationButtonState("checking", "Checking current location")');
    expect(guideMap).toContain('setLocationButtonState("near", "Center map on current location")');
    expect(guideMap).toContain("watchPosition");
    expect(css).toContain(".map-icon-button");
    expect(css).toContain('.map-icon-button[aria-disabled="true"]');
    expect(css).toContain('.map-icon-button[data-location-state="checking"]');
    expect(css).toContain(".map-icon-button[data-busy=\"true\"] svg");
  });

  it("renders map pins from normalized place-type icons instead of generic circles", () => {
    const guideMap = readSource("src/components/GuideMap.astro");
    const css = readSource("src/styles/global.css");

    expect(guideMap).toContain("markerIcon: place.marker_icon");
    expect(guideMap).toContain("buildMapMarkerDataUrl");
    expect(guideMap).toContain("buildMapMarkerSvg");
    expect(guideMap).toContain('className: "guide-map-marker"');
    expect(guideMap).not.toContain("google.maps.SymbolPath.CIRCLE");
    expect(css).toContain(".guide-map-marker");
    expect(css).toContain(".guide-map-marker svg");
    expect(css).toContain("width: 100%;");
    expect(css).toContain("height: 100%;");
  });

  it("keeps the home guide map event contract in sync with the home browser", () => {
    const homePage = readSource("src/pages/index.astro");
    const homeMap = readSource("src/components/HomeGuideMap.astro");
    const homeBrowser = readSource("public/scripts/home-browser.js");

    expect(homePage).toContain("<HomeGuideMap guides={locationGuideCandidates} />");
    expect(homeMap).toContain("data-home-guide-map");
    expect(homeMap).toContain("data-guides={JSON.stringify(mapGuides).replace(/</g, \"\\\\u003c\")}");
    expect(homeBrowser).toContain('root.querySelector("[data-home-guide-map]")');
    expect(homeBrowser).toContain('new CustomEvent("favorite-places:home-map-update"');
    expect(homeMap).toContain('document.addEventListener(\n      "favorite-places:home-map-update"');
    expect(homeMap).toContain("pendingState");
    expect(homeMap).toContain("runtime?.setVisibleGuides(currentVisibleGuideSlugs)");
    expect(homeMap).toContain("runtime?.fitGuides(currentVisibleGuides)");
    expect(homeMap).toContain("applyVisibility(\n      pendingState.visibleGuideSlugs,");
  });
});
