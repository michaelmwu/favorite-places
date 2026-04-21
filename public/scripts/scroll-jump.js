import {
  getScrollBehavior,
  getScrollControlAnchor,
  isScrollControlDisabled,
  MAP_PANEL_SELECTOR,
  shouldShowScrollControl,
} from "./scroll-jump-core.js";

const prefersReducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
const scrollBehavior = getScrollBehavior(prefersReducedMotion);
const mapPanels = [...document.querySelectorAll(MAP_PANEL_SELECTOR)];

const controls = document.createElement("div");
controls.className = "floating-scroll-controls";
controls.dataset.anchor = "right";
controls.hidden = true;

const topButton = document.createElement("button");
topButton.type = "button";
topButton.className = "floating-scroll-button";
topButton.setAttribute("aria-label", "Scroll to top");
topButton.textContent = "↑ Top";

controls.append(topButton);
document.body.append(controls);

const getMaxScroll = () => Math.max(document.documentElement.scrollHeight - window.innerHeight, 0);

const updateAnchor = () => {
  controls.dataset.anchor = getScrollControlAnchor(mapPanels);
};

const updateVisibility = () => {
  const scrollTop = window.scrollY || document.documentElement.scrollTop;
  const maxScroll = getMaxScroll();
  const shouldShow = shouldShowScrollControl({ maxScroll, scrollTop });

  controls.hidden = !shouldShow;

  if (!shouldShow) {
    return;
  }

  topButton.disabled = isScrollControlDisabled(scrollTop);
};

topButton.addEventListener("click", () => {
  window.scrollTo({ top: 0, behavior: scrollBehavior });
});

window.addEventListener("scroll", updateVisibility, { passive: true });
window.addEventListener("resize", () => {
  updateAnchor();
  updateVisibility();
});

for (const mapPanel of mapPanels) {
  new MutationObserver(() => {
    updateAnchor();
    updateVisibility();
  }).observe(mapPanel, {
    attributeFilter: ["data-collapsed"],
    attributes: true,
  });
}

updateAnchor();
updateVisibility();
