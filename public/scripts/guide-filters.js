const root = document.querySelector("[data-guide-root]");

if (root) {
  const cards = Array.from(root.querySelectorAll("[data-place-card]"));
  const list = root.querySelector("[data-place-list]");
  const searchInput = root.querySelector("[data-search-input]");
  const sortSelect = root.querySelector("[data-sort-select]");
  const tagButtons = Array.from(root.querySelectorAll("[data-tag-filter]"));
  const resultsCount = root.querySelector("[data-results-count]");
  const emptyState = root.querySelector("[data-empty-state]");
  const mapFilterStatus = root.querySelector("[data-map-filter-status]");
  const mapFilterResetButtons = Array.from(root.querySelectorAll("[data-map-filter-reset]"));

  let activeTag = "";
  let mapFramePlaceIds = null;

  const sorters = {
    curated: (left, right) => {
      const leftTopPick = left.dataset.topPick === "true" ? 1 : 0;
      const rightTopPick = right.dataset.topPick === "true" ? 1 : 0;
      if (leftTopPick !== rightTopPick) return rightTopPick - leftTopPick;
      return Number(right.dataset.rank || 0) - Number(left.dataset.rank || 0)
        || (left.dataset.name || "").localeCompare(right.dataset.name || "");
    },
    name: (left, right) => (left.dataset.name || "").localeCompare(right.dataset.name || ""),
    neighborhood: (left, right) =>
      (left.dataset.neighborhood || "").localeCompare(right.dataset.neighborhood || "")
      || (left.dataset.name || "").localeCompare(right.dataset.name || ""),
  };

  const clearMapFrameFilter = () => {
    mapFramePlaceIds = null;
    update("map-reset");
    root.dispatchEvent(new CustomEvent("guide:map-frame-reset", {
      bubbles: true,
    }));
  };

  const update = (source = "list-filter") => {
    const query = (searchInput?.value || "").trim().toLowerCase();
    const sort = sortSelect?.value || "curated";

    const visibleCards = cards.filter((card) => {
      const matchesQuery = !query || (card.dataset.search || "").includes(query);
      const matchesTag = !activeTag || (card.dataset.tags || "").split(" ").includes(activeTag);
      const matchesMapFrame = !mapFramePlaceIds || mapFramePlaceIds.has(card.dataset.placeId || "");
      const visible = matchesQuery && matchesTag && matchesMapFrame;
      card.hidden = !visible;
      return visible;
    });

    visibleCards.sort(sorters[sort] || sorters.curated).forEach((card) => {
      list?.appendChild(card);
    });

    if (resultsCount) {
      const suffix = mapFramePlaceIds ? " in map view" : "";
      resultsCount.textContent = `${visibleCards.length} place${visibleCards.length === 1 ? "" : "s"}${suffix}`;
    }

    if (emptyState) {
      emptyState.dataset.visible = visibleCards.length === 0 ? "true" : "false";
    }

    if (mapFilterStatus) {
      mapFilterStatus.hidden = !mapFramePlaceIds;
    }

    mapFilterResetButtons.forEach((button) => {
      button.hidden = !mapFramePlaceIds;
    });

    root.dispatchEvent(new CustomEvent("guide:places-updated", {
      bubbles: true,
      detail: {
        source,
        mapFrameActive: Boolean(mapFramePlaceIds),
        visiblePlaceIds: visibleCards.map((card) => card.dataset.placeId).filter(Boolean),
      },
    }));
  };

  searchInput?.addEventListener("input", () => update());
  sortSelect?.addEventListener("change", () => update());
  tagButtons.forEach((button) => {
    button.addEventListener("click", () => {
      activeTag = button.dataset.tag || "";
      tagButtons.forEach((candidate) => {
        candidate.dataset.active = candidate === button ? "true" : "false";
      });
      update();
    });
  });

  root.addEventListener("guide:map-frame-filter", (event) => {
    const visiblePlaceIds = Array.isArray(event.detail?.visiblePlaceIds) ? event.detail.visiblePlaceIds : [];
    mapFramePlaceIds = new Set(visiblePlaceIds);
    update("map-frame");
  });

  root.addEventListener("guide:map-frame-reset-request", clearMapFrameFilter);

  mapFilterResetButtons.forEach((button) => {
    button.addEventListener("click", clearMapFrameFilter);
  });

  const allButton = tagButtons.find((button) => (button.dataset.tag || "") === "");
  if (allButton) {
    allButton.dataset.active = "true";
  }
  update();
}
