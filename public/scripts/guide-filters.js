import { loadSearchIndex, searchPlaces } from "./place-search.js";

function getTagComparisonValue(value) {
  const normalizedText = String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
  const slug = normalizedText.replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");

  return slug || normalizedText;
}

function parseCardTagValues(value) {
  if (!value) {
    return [];
  }

  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed)
      ? parsed.map((item) => getTagComparisonValue(item)).filter(Boolean)
      : [];
  } catch {
    return String(value)
      .split(/\s+/)
      .map((item) => getTagComparisonValue(item))
      .filter(Boolean);
  }
}

function toRadians(degrees) {
  return (degrees * Math.PI) / 180;
}

function distanceInKm(fromLat, fromLng, toLat, toLng) {
  const earthRadiusKm = 6371;
  const latDelta = toRadians(toLat - fromLat);
  const lngDelta = toRadians(toLng - fromLng);
  const fromLatRadians = toRadians(fromLat);
  const toLatRadians = toRadians(toLat);
  const a =
    Math.sin(latDelta / 2) ** 2 +
    Math.cos(fromLatRadians) * Math.cos(toLatRadians) * Math.sin(lngDelta / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

  return earthRadiusKm * c;
}

function getCardCoordinates(card) {
  const lat = card.dataset.lat ? Number(card.dataset.lat) : Number.NaN;
  const lng = card.dataset.lng ? Number(card.dataset.lng) : Number.NaN;

  return Number.isFinite(lat) && Number.isFinite(lng) ? { lat, lng } : null;
}

export function buildNearbyDistanceMap(cards, currentLocation) {
  const distances = new Map();
  if (!currentLocation) {
    return distances;
  }

  cards.forEach((card) => {
    const coordinates = getCardCoordinates(card);
    if (!coordinates) {
      return;
    }

    distances.set(
      card.dataset.placeId || "",
      distanceInKm(currentLocation.lat, currentLocation.lng, coordinates.lat, coordinates.lng),
    );
  });

  return distances;
}

export function compareCardsByCurated(left, right) {
  const leftTopPick = left.dataset.topPick === "true" ? 1 : 0;
  const rightTopPick = right.dataset.topPick === "true" ? 1 : 0;
  if (leftTopPick !== rightTopPick) return rightTopPick - leftTopPick;
  return (
    Number(right.dataset.rank || 0) - Number(left.dataset.rank || 0) ||
    (left.dataset.name || "").localeCompare(right.dataset.name || "")
  );
}

export function compareCardsByNearby(
  left,
  right,
  { currentLocation = null, distanceByPlaceId = new Map() } = {},
) {
  if (!currentLocation) {
    return compareCardsByCurated(left, right);
  }

  const leftDistance = distanceByPlaceId.get(left.dataset.placeId || "");
  const rightDistance = distanceByPlaceId.get(right.dataset.placeId || "");
  const leftHasDistance = Number.isFinite(leftDistance);
  const rightHasDistance = Number.isFinite(rightDistance);

  if (leftHasDistance !== rightHasDistance) {
    return leftHasDistance ? -1 : 1;
  }

  if (!leftHasDistance || !rightHasDistance) {
    return compareCardsByCurated(left, right);
  }

  return leftDistance - rightDistance || compareCardsByCurated(left, right);
}

export function resolveLocationSortState({
  fallbackMessage = "Location unavailable. Showing curated order instead.",
  fallbackSortValue = "curated",
  currentLocation = null,
  currentLocationStatus = "idle",
  sortValue,
} = {}) {
  const shouldFallback =
    sortValue === "nearby" &&
    !currentLocation &&
    currentLocationStatus !== "idle" &&
    currentLocationStatus !== "checking";

  if (!shouldFallback) {
    return {
      message: "",
      shouldFallback: false,
      sortValue,
    };
  }

  return {
    message: fallbackMessage,
    shouldFallback: true,
    sortValue: fallbackSortValue,
  };
}

export function cardHasTag(card, tag) {
  const normalizedTag = getTagComparisonValue(tag);
  if (!normalizedTag) {
    return true;
  }

  return [
    ...parseCardTagValues(card.dataset.tags),
    ...parseCardTagValues(card.dataset.vibeTags),
  ].includes(normalizedTag);
}

export function matchesCardSearch(card, { normalizedQuery = "", searchResultIds = null } = {}) {
  return searchResultIds
    ? searchResultIds.has(card.dataset.placeId || "")
    : !normalizedQuery || (card.dataset.search || "").includes(normalizedQuery);
}

function getCardAreaComparisonValue(card) {
  return getTagComparisonValue(card.dataset.neighborhood || "");
}

export function cardMatchesType(card, { activeTypeValue = "", activeTypeSeedValues = [] } = {}) {
  const normalizedType = getTagComparisonValue(activeTypeValue);
  const normalizedCardCategory = getTagComparisonValue(card.dataset.category || "");

  return (
    !normalizedType ||
    normalizedCardCategory === normalizedType ||
    activeTypeSeedValues.some((seedTag) => cardHasTag(card, seedTag))
  );
}

export function countMatchingCards(
  cards,
  {
    activeArea = "",
    mapFramePlaceIds = null,
    normalizedQuery = "",
    searchResultIds = null,
    tag = "",
  } = {},
) {
  const normalizedActiveArea = getTagComparisonValue(activeArea);
  return cards.filter((card) => {
    const matchesSearch = matchesCardSearch(card, { normalizedQuery, searchResultIds });
    const matchesArea =
      !normalizedActiveArea || getCardAreaComparisonValue(card) === normalizedActiveArea;
    const matchesMapFrame = !mapFramePlaceIds || mapFramePlaceIds.has(card.dataset.placeId || "");
    return matchesSearch && matchesArea && matchesMapFrame && cardHasTag(card, tag);
  }).length;
}

export function buildAreaFilterStatusMessage({ activeAreaLabel, visibleCount, overflowCount }) {
  if (!activeAreaLabel || overflowCount <= 0 || visibleCount <= 0) {
    return "";
  }

  return `Showing ${visibleCount} place${visibleCount === 1 ? "" : "s"} in ${activeAreaLabel}. ${overflowCount} more match${overflowCount === 1 ? "" : "es"} elsewhere in this guide.`;
}

export function buildEmptyStateMessage({ activeAreaLabel = "", overflowCount = 0, query = "" }) {
  const matchWord = overflowCount === 1 ? "match" : "matches";

  if (query && activeAreaLabel && overflowCount > 0) {
    return `No places matched "${query}" in ${activeAreaLabel}. ${overflowCount} more ${matchWord} elsewhere in this guide. Try another area or clear the area filter.`;
  }
  if (query && activeAreaLabel) {
    return `No places matched "${query}" in ${activeAreaLabel}. Try another area or clear the area filter.`;
  }
  if (activeAreaLabel && overflowCount > 0) {
    return `No matches in ${activeAreaLabel}. ${overflowCount} more ${matchWord} elsewhere in this guide. Try another area or clear the area filter.`;
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
  const areaButtons = Array.from(root.querySelectorAll("[data-area-filter]"));
  const typeButtons = Array.from(root.querySelectorAll("[data-type-filter]"));
  const selectedTagsRow = root.querySelector("[data-selected-tags]");
  const suggestionList = root.querySelector("[data-suggestion-list]");
  const suggestionGroup = suggestionList?.closest(".control-group") || null;
  const autocomplete = root.querySelector("[data-tag-autocomplete]");
  const resultsCount = root.querySelector("[data-results-count]");
  const emptyState = root.querySelector("[data-empty-state]");
  const areaFilterStatus = root.querySelector("[data-area-filter-status]");
  const locationSortStatus = root.querySelector("[data-location-sort-status]");
  const mapFilterStatus = root.querySelector("[data-map-filter-status]");
  const mapFilterResetButtons = Array.from(root.querySelectorAll("[data-map-filter-reset]"));
  const guideSlug = root.dataset.guideSlug || "";
  const hasMappablePlaces = root.dataset.hasMappablePlaces === "true";
  const locationSortFallbackText =
    root.dataset.locationSortFallbackText || "Location unavailable. Showing curated order instead.";
  const initialParams = new URLSearchParams(window.location.search);
  const LOCATION_SORT_VALUE = "nearby";
  const LOCATION_SORT_FALLBACK = "curated";

  const normalizeTag = (value) => getTagComparisonValue(String(value || ""));
  const allTags = [
    ...new Set(
      JSON.parse(root.dataset.allTags || "[]")
        .map(normalizeTag)
        .filter(Boolean),
    ),
  ];
  const defaultSuggestions = [
    ...new Set(
      JSON.parse(root.dataset.defaultSuggestions || "[]")
        .map(normalizeTag)
        .filter(Boolean),
    ),
  ];
  const typeSeeds = Object.fromEntries(
    Object.entries(JSON.parse(root.dataset.typeSeeds || "{}")).map(([type, tags]) => [
      normalizeTag(type),
      [...new Set((Array.isArray(tags) ? tags : []).map(normalizeTag).filter(Boolean))],
    ]),
  );

  let activeArea = "";
  let activeType = "";
  let selectedTags = [];
  let mapFramePlaceIds = null;
  let searchIndex = null;
  let currentLocation = null;
  let currentLocationStatus = "idle";
  let nearbyDistanceByPlaceId = new Map();
  let hasHandledLocationRequest = false;
  let directLocationRequestInFlight = false;
  let directLocationFallbackTimer = null;
  const highlightedPlaceId = initialParams.get("place") || "";
  let autocompleteTags = [];
  let highlightedAutocompleteIndex = -1;

  const TAG_QUERY_PATTERN = /(?:^|\s)#([a-z0-9-]*)$/i;
  const COMPLETED_TAG_PATTERN = /(^|\s)#([a-z0-9-]+)(?=\s)/gi;

  const escapeHtml = (value) =>
    value
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;");

  const getAutocompleteState = (value) => {
    const match = value.match(TAG_QUERY_PATTERN);
    if (!match) {
      return null;
    }

    const hashIndex = value.lastIndexOf("#");
    if (hashIndex < 0) {
      return null;
    }

    return {
      hashIndex,
      fragment: match[1].toLowerCase(),
    };
  };

  const getSanitizedQuery = (value) => {
    const autocompleteState = getAutocompleteState(value);
    const baseValue = autocompleteState ? value.slice(0, autocompleteState.hashIndex) : value;
    return baseValue.trim();
  };

  const consumeCompletedTags = () => {
    if (!searchInput) {
      return;
    }

    const recognizedTags = [];
    const nextValue = searchInput.value
      .replace(COMPLETED_TAG_PATTERN, (match, prefix, tag) => {
        const normalizedTag = normalizeTag(tag);
        if (!normalizedTag || !allTags.includes(normalizedTag)) {
          return match;
        }

        recognizedTags.push(normalizedTag);
        return prefix;
      })
      .replace(/\s{2,}/g, " ")
      .trimStart();

    if (recognizedTags.length === 0) {
      return;
    }

    searchInput.value = nextValue;
    recognizedTags.forEach((tag) => addTag(tag));
  };

  const updateSuggestions = () => {
    if (!suggestionList || !suggestionGroup) {
      return;
    }

    const showSuggestions = !activeType;
    suggestionGroup.hidden = !showSuggestions;
    if (!showSuggestions) {
      suggestionList.innerHTML = "";
      return;
    }

    const suggestions = defaultSuggestions.filter((tag) => !selectedTags.includes(tag));
    suggestionList.innerHTML = suggestions
      .map(
        (tag) => `
          <button class="tag-pill" type="button" data-suggestion-tag="${escapeHtml(tag)}">
            #${escapeHtml(tag)}
            <span class="visually-hidden" data-tag-count-text></span>
          </button>
        `,
      )
      .join("");
  };

  const updateSelectedTags = () => {
    if (!selectedTagsRow) {
      return;
    }

    if (selectedTags.length === 0) {
      selectedTagsRow.hidden = true;
      selectedTagsRow.innerHTML = "";
      return;
    }

    selectedTagsRow.hidden = false;
    selectedTagsRow.innerHTML = selectedTags
      .map(
        (tag) => `
          <span class="selected-tag-chip">
            <span class="selected-tag-chip-label">#${escapeHtml(tag)}</span>
            <button class="selected-tag-chip-remove" type="button" data-remove-tag="${escapeHtml(tag)}" aria-label="Remove #${escapeHtml(tag)}">
              ×
            </button>
          </span>
        `,
      )
      .join("");
  };

  const closeAutocomplete = () => {
    autocompleteTags = [];
    highlightedAutocompleteIndex = -1;
    if (autocomplete) {
      autocomplete.hidden = true;
      autocomplete.innerHTML = "";
    }
  };

  const updateAutocomplete = () => {
    if (!autocomplete || !searchInput) {
      return;
    }

    const autocompleteState = getAutocompleteState(searchInput.value);
    if (!autocompleteState) {
      closeAutocomplete();
      return;
    }

    const fragment = autocompleteState.fragment;
    autocompleteTags = allTags
      .filter((tag) => !selectedTags.includes(tag) && (!fragment || tag.startsWith(fragment)))
      .slice(0, 8);

    if (autocompleteTags.length === 0) {
      closeAutocomplete();
      return;
    }

    highlightedAutocompleteIndex = Math.min(
      highlightedAutocompleteIndex < 0 ? 0 : highlightedAutocompleteIndex,
      autocompleteTags.length - 1,
    );

    autocomplete.hidden = false;
    autocomplete.innerHTML = autocompleteTags
      .map((tag, index) => {
        const active = index === highlightedAutocompleteIndex ? ' data-active="true"' : "";
        return `<button class="tag-autocomplete-option" type="button" data-autocomplete-tag="${escapeHtml(tag)}"${active}>#${escapeHtml(tag)}</button>`;
      })
      .join("");
  };

  const addTag = (tag) => {
    const normalizedTag = normalizeTag(tag);
    if (!normalizedTag || selectedTags.includes(normalizedTag)) {
      return;
    }

    selectedTags = [...selectedTags, normalizedTag];
    updateSelectedTags();
    updateSuggestions();
  };

  const removeTag = (tag) => {
    const normalizedTag = normalizeTag(tag);
    selectedTags = selectedTags.filter((candidate) => candidate !== normalizedTag);
    updateSelectedTags();
    updateSuggestions();
  };

  const applyAutocompleteTag = (tag) => {
    if (!searchInput) {
      return;
    }

    const autocompleteState = getAutocompleteState(searchInput.value);
    if (autocompleteState) {
      searchInput.value = searchInput.value
        .slice(0, autocompleteState.hashIndex)
        .replace(/\s+$/, "");
    }

    addTag(tag);
    closeAutocomplete();
    update();
    searchInput.focus();
  };

  const sorters = {
    curated: compareCardsByCurated,
    nearby: (left, right) =>
      compareCardsByNearby(left, right, {
        currentLocation,
        distanceByPlaceId: nearbyDistanceByPlaceId,
      }),
    rating: (left, right) =>
      Number(right.dataset.rating || 0) - Number(left.dataset.rating || 0) ||
      Number(right.dataset.ratingCount || 0) - Number(left.dataset.ratingCount || 0) ||
      sorters.curated(left, right),
    name: (left, right) => (left.dataset.name || "").localeCompare(right.dataset.name || ""),
    neighborhood: (left, right) =>
      (left.dataset.neighborhood || "").localeCompare(right.dataset.neighborhood || "") ||
      (left.dataset.name || "").localeCompare(right.dataset.name || ""),
  };

  const cardMatchesFilters = (
    card,
    {
      activeAreaLabelValue = "",
      activeTypeValue = "",
      activeTypeSeedValues = [],
      mapFrameIds = null,
      normalizedQuery = "",
      searchResultIds = null,
      selectedTagValues = [],
    } = {},
  ) => {
    const matchesSearch = matchesCardSearch(card, { normalizedQuery, searchResultIds });
    const matchesArea =
      !activeAreaLabelValue || getCardAreaComparisonValue(card) === activeAreaLabelValue;
    const matchesMapFrame = !mapFrameIds || mapFrameIds.has(card.dataset.placeId || "");
    const matchesSelectedTags = selectedTagValues.every((tag) => cardHasTag(card, tag));
    const matchesType = cardMatchesType(card, {
      activeTypeValue,
      activeTypeSeedValues,
    });
    return matchesSearch && matchesArea && matchesMapFrame && matchesSelectedTags && matchesType;
  };

  const countFilteredCards = (filters) =>
    cards.filter((card) => cardMatchesFilters(card, filters)).length;

  const compareHighlightedPlace = (left, right) => {
    if (!highlightedPlaceId) {
      return 0;
    }

    const leftIsHighlighted = left.dataset.placeId === highlightedPlaceId;
    const rightIsHighlighted = right.dataset.placeId === highlightedPlaceId;

    if (leftIsHighlighted === rightIsHighlighted) {
      return 0;
    }

    return leftIsHighlighted ? -1 : 1;
  };

  const clearMapFrameFilter = () => {
    mapFramePlaceIds = null;
    update("map-reset");
    root.dispatchEvent(
      new CustomEvent("guide:map-frame-reset", {
        bubbles: true,
      }),
    );
  };

  const setLocationSortMessage = (message = "") => {
    if (!locationSortStatus) {
      return;
    }

    locationSortStatus.hidden = !message;
    locationSortStatus.textContent = message;
  };

  const clearDirectLocationFallbackTimer = () => {
    if (directLocationFallbackTimer !== null) {
      window.clearTimeout(directLocationFallbackTimer);
      directLocationFallbackTimer = null;
    }
  };

  const dispatchUserLocation = ({ coordinates = null, source = "filters", status }) => {
    root.dispatchEvent(
      new CustomEvent("guide:user-location", {
        bubbles: true,
        detail: {
          coordinates,
          nearGuide: false,
          source,
          status,
        },
      }),
    );
  };

  const requestCurrentLocationDirectly = () => {
    if (directLocationRequestInFlight) {
      return;
    }

    if (!navigator.geolocation) {
      dispatchUserLocation({ status: "unavailable" });
      return;
    }

    directLocationRequestInFlight = true;
    dispatchUserLocation({ status: "checking" });

    navigator.geolocation.getCurrentPosition(
      (position) => {
        directLocationRequestInFlight = false;
        clearDirectLocationFallbackTimer();
        dispatchUserLocation({
          coordinates: {
            lat: position.coords.latitude,
            lng: position.coords.longitude,
          },
          status: "available",
        });
      },
      (error) => {
        directLocationRequestInFlight = false;
        clearDirectLocationFallbackTimer();
        dispatchUserLocation({
          status: error.code === error.PERMISSION_DENIED ? "denied" : "unavailable",
        });
      },
      {
        maximumAge: 300_000,
        timeout: 10_000,
      },
    );
  };

  const requestCurrentLocation = () => {
    hasHandledLocationRequest = false;
    clearDirectLocationFallbackTimer();
    root.dispatchEvent(
      new CustomEvent("guide:user-location-request", {
        bubbles: true,
      }),
    );

    if (!hasMappablePlaces || !document.querySelector("[data-guide-map]")) {
      requestCurrentLocationDirectly();
      return;
    }

    directLocationFallbackTimer = window.setTimeout(() => {
      if (!hasHandledLocationRequest) {
        requestCurrentLocationDirectly();
      }
    }, 500);
  };

  const refreshNearbyDistances = () => {
    nearbyDistanceByPlaceId = buildNearbyDistanceMap(cards, currentLocation);
  };

  const update = (source = "list-filter") => {
    const query = searchInput ? getSanitizedQuery(searchInput.value) : "";
    const normalizedQuery = query.toLowerCase();
    const sort = sortSelect?.value || "curated";
    const searchState =
      searchIndex && guideSlug
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
    const activeTypeSeeds = activeType ? typeSeeds[activeType] || [] : [];
    const activeAreaLabel = activeArea
      ? areaButtons.find((button) => (button.dataset.area || "") === activeArea)?.dataset
          .areaLabel || ""
      : "";

    const visibleCards = cards.filter((card) => {
      const visible = cardMatchesFilters(card, {
        activeAreaLabelValue: activeArea,
        activeTypeValue: activeType,
        activeTypeSeedValues: activeTypeSeeds,
        mapFrameIds: mapFramePlaceIds,
        normalizedQuery,
        searchResultIds,
        selectedTagValues: selectedTags,
      });
      card.hidden = !visible;
      card.dataset.searchHighlight =
        highlightedPlaceId && card.dataset.placeId === highlightedPlaceId ? "true" : "false";
      return visible;
    });

    const broaderAreaCount = activeArea
      ? countFilteredCards({
          activeTypeValue: activeType,
          activeTypeSeedValues: activeTypeSeeds,
          normalizedQuery,
          searchResultIds,
          selectedTagValues: selectedTags,
        })
      : visibleCards.length;
    const areaMatchCount = activeArea
      ? countFilteredCards({
          activeAreaLabelValue: activeArea,
          activeTypeValue: activeType,
          activeTypeSeedValues: activeTypeSeeds,
          normalizedQuery,
          searchResultIds,
          selectedTagValues: selectedTags,
        })
      : visibleCards.length;
    const areaOverflowCount = activeArea ? Math.max(0, broaderAreaCount - areaMatchCount) : 0;

    if (suggestionList) {
      suggestionList.querySelectorAll("[data-suggestion-tag]").forEach((button) => {
        const suggestionTag = button.getAttribute("data-suggestion-tag") || "";
        const countText = button.querySelector("[data-tag-count-text]");
        if (!countText) {
          return;
        }

        const count = countFilteredCards({
          activeAreaLabelValue: activeArea,
          activeTypeValue: activeType,
          activeTypeSeedValues: activeTypeSeeds,
          mapFrameIds: mapFramePlaceIds,
          normalizedQuery,
          searchResultIds,
          selectedTagValues: [...selectedTags, suggestionTag],
        });

        countText.textContent = ` (${count} match${count === 1 ? "" : "es"})`;
      });
    }

    const sorter =
      normalizedQuery && sort === "curated" && searchResultIds
        ? (left, right) =>
            (searchScores.get(right.dataset.placeId || "") || 0) -
              (searchScores.get(left.dataset.placeId || "") || 0) || sorters.curated(left, right)
        : sorters[sort] || sorters.curated;

    visibleCards
      .sort((left, right) => compareHighlightedPlace(left, right) || sorter(left, right))
      .forEach((card) => {
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

    root.dispatchEvent(
      new CustomEvent("guide:places-updated", {
        bubbles: true,
        detail: {
          source,
          mapFrameActive: Boolean(mapFramePlaceIds),
          visiblePlaceIds: visibleCards.map((card) => card.dataset.placeId).filter(Boolean),
        },
      }),
    );
  };

  searchInput?.addEventListener("input", () => {
    consumeCompletedTags();
    updateAutocomplete();
    update();
  });

  searchInput?.addEventListener("keydown", (event) => {
    if (autocompleteTags.length === 0) {
      return;
    }

    if (event.key === "ArrowDown") {
      event.preventDefault();
      highlightedAutocompleteIndex = (highlightedAutocompleteIndex + 1) % autocompleteTags.length;
      updateAutocomplete();
      return;
    }

    if (event.key === "ArrowUp") {
      event.preventDefault();
      highlightedAutocompleteIndex =
        (highlightedAutocompleteIndex - 1 + autocompleteTags.length) % autocompleteTags.length;
      updateAutocomplete();
      return;
    }

    if (event.key === "Enter") {
      event.preventDefault();
      applyAutocompleteTag(autocompleteTags[Math.max(highlightedAutocompleteIndex, 0)]);
      return;
    }

    if (event.key === "Escape") {
      closeAutocomplete();
    }
  });

  searchInput?.addEventListener("blur", () => {
    window.requestAnimationFrame(() => {
      closeAutocomplete();
    });
  });

  sortSelect?.addEventListener("change", () => {
    if (sortSelect.value === LOCATION_SORT_VALUE) {
      setLocationSortMessage("");
      if (!currentLocation) {
        requestCurrentLocation();
      }
    } else {
      setLocationSortMessage("");
    }

    update();
  });

  typeButtons.forEach((button) => {
    button.addEventListener("click", () => {
      activeType = button.dataset.type || "";
      typeButtons.forEach((candidate) => {
        candidate.dataset.active = candidate === button ? "true" : "false";
      });
      updateSuggestions();
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

  suggestionList?.addEventListener("click", (event) => {
    if (!(event.target instanceof Element)) {
      return;
    }

    const button = event.target.closest("[data-suggestion-tag]");
    if (!(button instanceof HTMLElement)) {
      return;
    }

    const tag = button.dataset.suggestionTag || "";
    if (!tag) {
      return;
    }

    if (selectedTags.includes(tag)) {
      removeTag(tag);
    } else {
      addTag(tag);
    }

    update();
    searchInput?.focus();
  });

  selectedTagsRow?.addEventListener("click", (event) => {
    if (!(event.target instanceof Element)) {
      return;
    }

    const button = event.target.closest("[data-remove-tag]");
    if (!(button instanceof HTMLElement)) {
      return;
    }

    const tag = button.dataset.removeTag || "";
    removeTag(tag);
    update();
    searchInput?.focus();
  });

  autocomplete?.addEventListener("mousedown", (event) => {
    event.preventDefault();
  });

  autocomplete?.addEventListener("click", (event) => {
    if (!(event.target instanceof Element)) {
      return;
    }

    const button = event.target.closest("[data-autocomplete-tag]");
    if (!(button instanceof HTMLElement)) {
      return;
    }

    const tag = button.dataset.autocompleteTag || "";
    applyAutocompleteTag(tag);
  });

  root.addEventListener("guide:map-frame-filter", (event) => {
    const visiblePlaceIds = Array.isArray(event.detail?.visiblePlaceIds)
      ? event.detail.visiblePlaceIds
      : [];
    mapFramePlaceIds = new Set(visiblePlaceIds);
    update("map-frame");
  });

  root.addEventListener("guide:map-frame-reset-request", clearMapFrameFilter);

  root.addEventListener("guide:user-location", (event) => {
    hasHandledLocationRequest = true;
    clearDirectLocationFallbackTimer();
    const detail = event.detail || {};
    const coordinates = detail.coordinates;
    currentLocation =
      coordinates &&
      Number.isFinite(Number(coordinates.lat)) &&
      Number.isFinite(Number(coordinates.lng))
        ? {
            lat: Number(coordinates.lat),
            lng: Number(coordinates.lng),
          }
        : null;
    currentLocationStatus = detail.status || "idle";
    refreshNearbyDistances();

    if (currentLocation) {
      setLocationSortMessage("");
      if (sortSelect?.value === LOCATION_SORT_VALUE) {
        update("location-sort");
      }
      return;
    }

    const nextLocationSortState = resolveLocationSortState({
      fallbackMessage: locationSortFallbackText,
      fallbackSortValue: LOCATION_SORT_FALLBACK,
      currentLocation,
      currentLocationStatus,
      sortValue: sortSelect?.value,
    });

    if (nextLocationSortState.shouldFallback && sortSelect) {
      sortSelect.value = nextLocationSortState.sortValue;
      setLocationSortMessage(nextLocationSortState.message);
      update("location-sort-fallback");
    }
  });

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

  const allTypeButton = typeButtons.find((button) => (button.dataset.type || "") === "");
  if (allTypeButton) {
    allTypeButton.dataset.active = "true";
  }
  const allAreaButton = areaButtons.find((button) => (button.dataset.area || "") === "");
  if (allAreaButton) {
    allAreaButton.dataset.active = "true";
  }

  updateSelectedTags();
  updateSuggestions();
  applyDeepLink();
  consumeCompletedTags();
  updateAutocomplete();
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
