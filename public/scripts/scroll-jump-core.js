export const SCROLL_SHOW_OFFSET = 700;
export const SCROLL_TOP_GAP = 80;
export const MAP_PANEL_SELECTOR = "[data-map-panel], [data-home-map-panel]";

export const getScrollBehavior = (prefersReducedMotion) =>
  prefersReducedMotion ? "auto" : "smooth";

export const shouldShowScrollControl = ({ maxScroll, scrollTop }) =>
  maxScroll > SCROLL_SHOW_OFFSET && scrollTop > SCROLL_SHOW_OFFSET;

export const isScrollControlDisabled = (scrollTop) => scrollTop < SCROLL_TOP_GAP;

export const getScrollControlAnchor = (mapPanels) =>
  mapPanels.some((panel) => panel.dataset.collapsed !== "true") ? "left" : "right";
