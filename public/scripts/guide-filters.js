import { loadSearchIndex, searchPlaces } from "./place-search.js";

export function cardHasTag(card, tag) {
  const normalizedTag = String(tag || "").trim().toLowerCase();
  if (!normalizedTag) {
    return true;
  }

  return `${card.dataset.tags || ""} ${card.dataset.vibeTags || ""}`
    .split(/\s+/)
    .includes(normalizedTag);
}

export function matchesCardSearch(card, { normalizedQuery = "", searchResultIds = null } = {}) {
  return searchResultIds
    ? searchResultIds.has(card.dataset.placeId || "")
    : !normalizedQuery || (card.dataset.search || "").includes(normalizedQuery);
}

export function countMatchingCards(cards, {
  activeArea = "",
  mapFramePlaceIds = null,
  normalizedQuery = "",
  searchResultIds = null,
  tag = "",
} = {}) {
  return cards.filter((card) => {
    const matchesSearch = matchesCardSearch(card, { normalizedQuery, searchResultIds });
    const matchesArea = !activeArea || (card.dataset.neighborhood || "") === activeArea;
    const matchesMapFrame = !mapFramePlaceIds || mapFramePlaceIds.has(card.dataset.placeId || "");
    return matchesSearch && matchesArea && matchesMapFrame && cardHasTag(card, tag);
  }).length;
}

export function buildAreaFilterStatusMessage({ activeAreaLabel, visibleCount, overflowCount }) {
  if (!activeAreaLabel || overflowCount <= 0) {
    return "";
  }

  return `Showing ${visibleCount} place${visibleCount === 1 ? "" : "s"} in ${activeAreaLabel}. ${overflowCount} more match elsewhere in this guide.`;
}

export function buildEmptyStateMessage({ activeAreaLabel = "", overflowCount = 0, query = "" }) {
  if (query && activeAreaLabel && overflowCount > 0) {
    return `No places matched "${query}" in ${activeAreaLabel}. ${overflowCount} more match elsewhere in this guide. Try another area or clear the area filter.`;
  }
  if (activeAreaLabel && overflowCount > 0) {
    return `No matches in ${activeAreaLabel}. ${overflowCount} more match elsewhere in this guide. Try another area or clear the area filter.`;
  }
  if (query) {
    return `No places matched "${query}" in this guide. Try a broader search or clear filters.`;
  }
  if (activeAreaLabel) {
    return `No matches in ${activeAreaLabel}. Try another area or clear the area filter.`;
  }
  return "No matches. Try a broader search or clear the tag filter.";
}

const root = typeof document === "undefined" ? null : document.querySelector("[data-guide-root]");

if (root) {
  const cards = Array.from(root.querySelectorAll("[data-place-card]"));
  const list = root.querySelector("[data-place-list]");
  const searchInput = root.querySelector("[data-search-input]");
  const sortSelect = root.querySelector("[data-sort-select]");
  const tagButtons = Array.from(root.querySelectorAll("[data-tag-filter]"));
  const areaButtons = Array.from(root.querySelectorAll("[data-area-filter]"));
  const resultsCount = root.querySelector("[data-results-count]");
  const emptyState = root.querySelector("[data-empty-state]");
  const areaFilterStatus = root.querySelector("[data-area-filter-status]");
  const mapFilterStatus = root.querySelector("[data-map-filter-status]");
  const mapFilterResetButtons = Array.from(root.querySelectorAll("[data-map-filter-reset]"));
  const guideSlug = root.dataset.guideSlug || "";
  const initialParams = new URLSearchParams(window.location.search);

  let activeTag = "";
  let activeArea = "";
  let mapFramePlaceIds = null;
  let searchIndex = null;
  let highlightedPlaceId = initialParams.get("place") || "";

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
    const query = (searchInput?.value || "").trim();
    const normalizedQuery = query.toLowerCase();
    const sort = sortSelect?.value || "curated";
    const searchState = searchIndex && guideSlug
      ? searchPlaces(query, {
        index: searchIndex,
        scope: "guide",
        guideSlug,
      })
      : null;
    const searchResultIds = searchState
      ? new Set(searchState.results.map((result) => result.entry.id))
      : null;
    const searchScores = searchState
      ? new Map(searchState.results.map((result) => [result.entry.id, result.score]))
      : new Map();

    const activeAreaLabel = areaButtons.find((button) => (button.dataset.area || "") === activeArea)?.dataset.areaLabel || "";
    const visibleCards = cards.filter((card) => {
      const matchesSearch = matchesCardSearch(card, { normalizedQuery, searchResultIds });
      const matchesTag = cardHasTag(card, activeTag);
      const matchesArea = !activeArea || (card.dataset.neighborhood || "") === activeArea;
      const matchesMapFrame = !mapFramePlaceIds || mapFramePlaceIds.has(card.dataset.placeId || "");
      const visible = matchesSearch && matchesTag && matchesArea && matchesMapFrame;
      card.hidden = !visible;
      card.dataset.searchHighlight = highlightedPlaceId && card.dataset.placeId === highlightedPlaceId ? "true" : "false";
      return visible;
    });
    const broaderAreaCount = activeArea
      ? countMatchingCards(cards, {
        mapFramePlaceIds,
        normalizedQuery,
        searchResultIds,
        tag: activeTag,
      })
      : visibleCards.length;
    const areaOverflowCount = activeArea ? Math.max(0, broaderAreaCount - visibleCards.length) : 0;

    tagButtons.forEach((button) => {
      const tag = button.dataset.tag || "";
      const count = countMatchingCards(cards, {
        activeArea,
        mapFramePlaceIds,
        normalizedQuery,
        searchResultIds,
        tag,
      });
      button.dataset.tagCount = String(count);
      const countText = button.querySelector("[data-tag-count-text]");
      if (countText) {
        countText.textContent = String(count);
      }
    });

    const sorter = normalizedQuery && sort === "curated" && searchResultIds
      ? (left, right) =>
        (searchScores.get(right.dataset.placeId || "") || 0)
          - (searchScores.get(left.dataset.placeId || "") || 0)
        || sorters.curated(left, right)
      : sorters[sort] || sorters.curated;

    visibleCards.sort(sorter).forEach((card) => {
      list?.appendChild(card);
    });

    if (resultsCount) {
      const suffix = mapFramePlaceIds ? " in map view" : "";
      resultsCount.textContent = `${visibleCards.length} place${visibleCards.length === 1 ? "" : "s"}${suffix}`;
    }

    if (emptyState) {
      emptyState.dataset.visible = visibleCards.length === 0 ? "true" : "false";
      emptyState.textContent = buildEmptyStateMessage({
        activeAreaLabel,
        overflowCount: areaOverflowCount,
        query,
      });
    }

    if (areaFilterStatus) {
      const message = buildAreaFilterStatusMessage({
        activeAreaLabel,
        visibleCount: visibleCards.length,
        overflowCount: areaOverflowCount,
      });
      areaFilterStatus.hidden = !message;
      areaFilterStatus.textContent = message;
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
  areaButtons.forEach((button) => {
    button.addEventListener("click", () => {
      activeArea = button.dataset.area || "";
      areaButtons.forEach((candidate) => {
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

  const applyDeepLink = () => {
    const initialQuery = initialParams.get("q") || "";
    if (initialQuery && searchInput) {
      searchInput.value = initialQuery;
    }
  };

  const scrollToHighlightedPlace = () => {
    if (!highlightedPlaceId) {
      return;
    }
    const target = cards.find((card) => card.dataset.placeId === highlightedPlaceId);
    if (!target) {
      return;
    }
    target.scrollIntoView({ behavior: "smooth", block: "center" });
    target.focus({ preventScroll: true });
  };

  const allButton = tagButtons.find((button) => (button.dataset.tag || "") === "");
  if (allButton) {
    allButton.dataset.active = "true";
  }
  const allAreaButton = areaButtons.find((button) => (button.dataset.area || "") === "");
  if (allAreaButton) {
    allAreaButton.dataset.active = "true";
  }
  applyDeepLink();
  update();

  loadSearchIndex()
    .then((index) => {
      searchIndex = index;
      update();
      requestAnimationFrame(scrollToHighlightedPlace);
    })
    .catch(() => {
      update();
      requestAnimationFrame(scrollToHighlightedPlace);
    });
}
