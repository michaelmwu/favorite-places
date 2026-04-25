import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const readSource = (path) => readFileSync(path, "utf8");
const normalizeWhitespace = (value) => value.replace(/\s+/g, " ").trim();
const expectCssToContain = (css, snippet) => {
  expect(normalizeWhitespace(css)).toContain(normalizeWhitespace(snippet));
};
const expectCssNotToContain = (css, snippet) => {
  expect(normalizeWhitespace(css)).not.toContain(normalizeWhitespace(snippet));
};
const cssBlocks = (css, selector) => {
  return [...css.matchAll(/([^{}]+){([^}]*)}/g)]
    .filter((match) =>
      match[1]
        .split(",")
        .map((part) => part.trim())
        .includes(selector),
    )
    .map((match) => match[2]);
};

describe("guide map interactions", () => {
  it("keeps guide search controls in normal document flow while the map remains sticky", () => {
    const css = readSource("src/styles/global.css");

    expect(cssBlocks(css, ".controls-panel").join("\n")).not.toContain("position: sticky");
    expect(cssBlocks(css, ".country-browser").join("\n")).toContain("position: sticky");
    expectCssToContain(css, ".browse-layout > .map-panel {\n    order: 0;\n    position: sticky;");
  });

  it("uses an in-flow mobile map toolbar instead of overlaying the controls", () => {
    const guideMap = readSource("src/components/GuideMap.astro");
    const css = readSource("src/styles/global.css");

    expect(guideMap).toContain('<div class="map-toolbar">');
    expect(guideMap).toContain('<span class="map-toggle-label">Hide map</span>');
    expectCssToContain(css, ".map-toolbar");
    expect(cssBlocks(css, ".map-panel").join("\n")).toContain("position: relative");
    expect(cssBlocks(css, ".map-actions").join("\n")).toContain("margin-left: auto");
    expectCssToContain(css, "@media (max-width: 979px)");
    expectCssToContain(css, ".map-actions {\n    justify-content: flex-start;");
    expectCssToContain(css, ".guide-map {\n  width: 100%;\n  min-height: 16rem;");
    expectCssToContain(css, ".map-panel {\n    order: 0;\n    position: sticky;\n    top: 0.5rem;");
  });

  it("tightens the mobile guide chrome to save vertical space", () => {
    const css = readSource("src/styles/global.css");

    expectCssToContain(css, ".hero p,\n.lede {\n  margin: 0;");
    expectCssToContain(
      css,
      "@media (max-width: 719px) {\n  .page-shell,\n  .ui-page-shell {\n    width: min(calc(100% - 0.5rem), var(--page-width));",
    );
    expectCssToContain(css, ".site-header,\n  .ui-site-header {\n    padding: 0.35rem 0 0.2rem;");
    expectCssToContain(
      css,
      ".site-title img,\n  .ui-site-title img {\n    width: min(84px, 22vw);",
    );
    expectCssToContain(css, ".site-nav,\n  .ui-site-nav {\n    gap: 0.6rem;");
    expectCssToContain(css, ".hero {\n    padding-bottom: 1.4rem;");
    expectCssToContain(css, ".hero-grid,\n  .section-stack {\n    gap: 0.55rem;");
    expectCssToContain(css, ".stat-pill,\n  .action-pill,\n  .tag-pill {\n    min-height: 2rem;");
    expectCssToContain(css, ".section {\n    margin-top: 1.5rem;");
    expectCssToContain(css, ".social-card {\n    padding: 0.6rem;");
    expectCssToContain(css, ".social-card-copy {\n    font-size: 0.88rem;");
    expectCssToContain(css, ".social-card-link {\n    padding: 0.42rem 0.45rem;");
    expectCssToContain(css, ".social-card-icon {\n    width: 1.5rem;");
  });

  it("renders the Google Maps action as a labeled pill beside the place name", () => {
    const placeCard = readSource("src/components/PlaceCard.astro");
    const css = readSource("src/styles/global.css");

    expect(placeCard).toContain(
      'import { buildMapMarkerSvg, getMapMarkerColors } from "../lib/mapMarkerIcons";',
    );
    expect(placeCard).toContain('class="place-card-name-row"');
    expect(placeCard).toContain('class="place-card-marker"');
    expect(placeCard).toContain('class="place-card-map-link"');
    expect(placeCard).toContain("aria-label={`Open ${place.name} in Google Maps`}");
    expect(placeCard).toContain('<img src="/icons/google-maps.svg" alt="" width="18" height="26"');
    expect(placeCard).toContain('class="place-card-map-link-label"');
    expect(placeCard).toContain("{siteConfig.placeCard.mapsLabel}</span>");
    expect(placeCard).not.toContain(">Open in Google Maps<");
    expect(placeCard).toContain("set:html={markerSvg}");
    expectCssToContain(css, ".place-card-map-link");
    expectCssToContain(css, ".place-card-map-link img");
    expectCssToContain(css, ".place-card-map-link-label");
    expectCssToContain(css, ".place-card-marker");
    expectCssToContain(css, ".place-card-name-row");
    expectCssToContain(css, "width: 1.8rem;");
    expectCssToContain(css, "display: inline-flex;");
    expectCssToContain(css, "padding: 0.32rem 0.62rem 0.32rem 0.42rem;");
    expectCssToContain(css, "background: rgba(255, 255, 255, 0.96);");
    expectCssToContain(css, "color: #18457a;");
    expectCssToContain(css, "border-radius: 999px;");
  });

  it("reflows place stats by card width instead of branching on photo presence", () => {
    const placeCard = readSource("src/components/PlaceCard.astro");
    const css = readSource("src/styles/global.css");

    expect(placeCard).toContain("(siteConfig.placeCard.showRating && ratingValue !== null)");
    expect(placeCard).toContain("(siteConfig.placeCard.showReviewCount && reviewCount !== null)");
    expect(placeCard).toContain('class="place-card-meta-row"');
    expect(placeCard).toContain(
      'class="stats-row ui-meta-row place-card-stats place-card-meta-stats"',
    );
    expect(placeCard).not.toContain("{hasPhoto && hasStats && (");
    expect(placeCard).not.toContain("{!hasPhoto && hasStats && (");
    expectCssToContain(css, ".place-card-meta-row");
    expectCssToContain(css, ".place-card-meta-stats");
    expectCssToContain(css, "container-type: inline-size;");
    expectCssToContain(css, "@container (min-width: 26rem)");
  });

  it("lets the collapsed map panel release width back to the places list", () => {
    const guidePage = readSource("src/pages/guides/[slug].astro");
    const homeMap = readSource("src/components/HomeGuideMap.astro");
    const guideMap = readSource("src/components/GuideMap.astro");
    const css = readSource("src/styles/global.css");

    expect(guidePage).toContain('class="browse-layout" data-browse-layout');
    expect(homeMap).toContain('class="map-panel home-map-panel"');
    expect(guidePage).toContain('class="browse-main" data-browse-main');
    expect(guidePage.indexOf('<div class="browse-main" data-browse-main>')).toBeLessThan(
      guidePage.indexOf("<GuideMap places={visiblePlaces} />"),
    );
    expect(guideMap).toContain('const DESKTOP_MAP_MEDIA_QUERY = "(min-width: 980px)"');
    expect(guideMap).toContain("const syncPanelOrder = (panel: HTMLElement) => {");
    expect(guideMap).toContain(
      'const browseMain = layout?.querySelector<HTMLElement>("[data-browse-main]")',
    );
    expect(guideMap).toContain("browseMain.after(panel);");
    expect(guideMap).toContain("layout.prepend(panel);");
    expect(guideMap).toContain('panel.closest<HTMLElement>("[data-browse-layout]")');
    expect(guideMap).toContain("layout.dataset.mapCollapsed");
    expectCssToContain(css, '.browse-layout[data-map-collapsed="true"]');
    expectCssToContain(css, "grid-template-columns: minmax(0, 1fr) 2.75rem");
    expectCssToContain(css, ".browse-layout > .map-panel {\n    order: 0;\n    position: sticky;");
    expectCssToContain(css, ".home-map-panel {\n  position: relative;\n  top: auto;");
    expectCssToContain(
      css,
      '.browse-layout[data-map-collapsed="true"] .card-grid[data-kind="places"]',
    );
    expectCssToContain(css, "grid-template-columns: repeat(auto-fit, minmax(23rem, 28rem))");
    expectCssToContain(css, "justify-content: start;");
    expect(cssBlocks(css, ".map-panel").join("\n")).toContain("order: 0;");
    expectCssToContain(css, '.map-panel[data-collapsed="true"] .map-toggle');
    expectCssToContain(css, "border-color: transparent");
    expectCssToContain(css, "background: transparent");
    expectCssNotToContain(css, "inset: 0;\n  width: 100%;\n  height: 100%;");
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
    expect(filters).toContain('new CustomEvent("guide:map-frame-reset"');
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
    expectCssToContain(css, ".map-icon-button");
    expectCssToContain(css, '.map-icon-button[aria-disabled="true"]');
    expectCssToContain(css, '.map-icon-button[data-location-state="checking"]');
    expectCssToContain(css, '.map-icon-button[data-busy="true"] svg');
  });

  it("renders map pins from normalized place-type icons instead of generic circles", () => {
    const guideMap = readSource("src/components/GuideMap.astro");
    const css = readSource("src/styles/global.css");

    expect(guideMap).toContain("markerIcon: place.marker_icon");
    expect(guideMap).toContain("buildMapMarkerDataUrl");
    expect(guideMap).toContain("buildMapMarkerSvg");
    expect(guideMap).toContain('className: "guide-map-marker"');
    expect(guideMap).not.toContain("google.maps.SymbolPath.CIRCLE");
    expectCssToContain(css, ".guide-map-marker");
    expectCssToContain(css, ".guide-map-marker svg");
    expectCssToContain(css, "width: 100%;");
    expectCssToContain(css, "height: 100%;");
  });

  it("keeps the home guide map event contract in sync with the home browser", () => {
    const homePage = readSource("src/pages/index.astro");
    const homeMap = readSource("src/components/HomeGuideMap.astro");
    const homeBrowser = readSource("public/scripts/home-browser.js");

    expect(homePage).toContain("<HomeGuideMap guides={locationGuideCandidates} />");
    expect(homePage).toContain(
      'data-country-sections-visible={siteConfig.home.showCountrySections ? "true" : "false"}',
    );
    expect(homePage).toContain(
      "{guidesByCountry.map(({ countryName, countryGuides, countryFlag }) => (",
    );
    expect(homeMap).toContain("data-home-guide-map");
    expect(homeMap).toContain('data-guides={JSON.stringify(mapGuides).replace(/</g, "\\\\u003c")}');
    expect(homeBrowser).toContain('root.querySelector("[data-home-guide-map]")');
    expect(homeBrowser).toContain(
      'const countrySectionsVisible = root.dataset.countrySectionsVisible !== "false";',
    );
    expect(homeBrowser).toContain(
      "block.hidden = !countrySectionsVisible || blockVisibleGuideCount === 0;",
    );
    expect(homeBrowser).toContain('new CustomEvent("favorite-places:home-map-update"');
    expect(homeMap).toContain(
      'document.addEventListener(\n      "favorite-places:home-map-update"',
    );
    expect(homeMap).toContain("pendingState");
    expect(homeMap).toContain("runtime?.setVisibleGuides(currentVisibleGuideSlugs)");
    expect(homeMap).toContain("runtime?.fitGuides(currentVisibleGuides)");
    expect(homeMap).toContain("applyVisibility(\n      pendingState.visibleGuideSlugs,");
  });

  it("constrains map panning before the world scrolls into grey tile space", () => {
    const homeMap = readSource("src/components/HomeGuideMap.astro");
    const guideMap = readSource("src/components/GuideMap.astro");

    expect(homeMap).toContain("const WORLD_MAP_MAX_LATITUDE = 85.05112878");
    expect(homeMap).toContain("const WORLD_MAP_MIN_ZOOM = 0");
    expect(homeMap).toContain("const WORLD_MAP_MIN_LONGITUDE = -180");
    expect(homeMap).toContain("const WORLD_MAP_MAX_LONGITUDE = 180");
    expect(homeMap).toContain("const WORLD_MAP_BOUNDS = {");
    expect(homeMap).toContain("east: WORLD_MAP_MAX_LONGITUDE");
    expect(homeMap).toContain("west: WORLD_MAP_MIN_LONGITUDE");
    expect(homeMap).toContain("const LEAFLET_WORLD_BOUNDS = L.latLngBounds");
    expect(homeMap).toContain("[-WORLD_MAP_MAX_LATITUDE, WORLD_MAP_MIN_LONGITUDE]");
    expect(homeMap).toContain("[WORLD_MAP_MAX_LATITUDE, WORLD_MAP_MAX_LONGITUDE]");
    expect(homeMap).toContain("maxBounds: LEAFLET_WORLD_BOUNDS");
    expect(homeMap).toContain("maxBoundsViscosity: 1");
    expect(homeMap).toContain("minZoom: WORLD_MAP_MIN_ZOOM");
    expect(homeMap).toContain("restriction: {");
    expect(homeMap).toContain("latLngBounds: WORLD_MAP_BOUNDS");
    expect(homeMap).toContain("strictBounds: true");

    expect(guideMap).not.toContain("const WORLD_MAP_MAX_LATITUDE = 85.05112878");
    expect(guideMap).not.toContain("const WORLD_MAP_MIN_ZOOM");
    expect(guideMap).not.toContain("const WORLD_MAP_BOUNDS = {");
    expect(guideMap).not.toContain("const LEAFLET_WORLD_BOUNDS = L.latLngBounds");
    expect(guideMap).not.toContain("maxBounds: LEAFLET_WORLD_BOUNDS");
    expect(guideMap).not.toContain("maxBoundsViscosity: 1");
    expect(guideMap).not.toContain("restriction: {");
    expect(guideMap).not.toContain("latLngBounds: WORLD_MAP_BOUNDS");
    expect(guideMap).not.toContain("strictBounds: true");
  });
});
