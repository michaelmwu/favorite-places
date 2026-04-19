import { loadSearchIndex, searchGuides, searchPlaces } from "./place-search.js";

const GROUP_LIMIT = 5;
const INDIVIDUAL_RESULT_LIMIT = 24;
const FLAG_SUFFIX_PATTERN = /(?:\s*[\u{1F1E6}-\u{1F1FF}]{2})+$/u;

const root = document.querySelector("[data-home-browser-root]");

if (root) {
  const searchInput = root.querySelector("[data-home-search-input]");
  const countryBlocks = Array.from(root.querySelectorAll("[data-country-block]"));
  const countryButtons = Array.from(root.querySelectorAll("[data-country-filter]"));
  const resultsCount = root.querySelector("[data-home-results-count]");
  const emptyState = root.querySelector("[data-home-empty-state]");
  const locationButton = root.querySelector("[data-location-target]");
  const locationStatus = root.querySelector("[data-location-status]");
  const homeGuideMap = root.querySelector("[data-home-guide-map]");
  const globalResults = root.querySelector("[data-global-search-results]");
  const globalResultsTitle = root.querySelector("[data-global-search-title]");
  const globalResultsSummary = root.querySelector("[data-global-search-summary]");
  const globalResultsToolbar = root.querySelector("[data-global-search-toolbar]");
  const globalResultsList = root.querySelector("[data-global-search-list]");
  const groupedResultsList = root.querySelector("[data-grouped-search-list]");
  const globalResultsEmpty = root.querySelector("[data-global-search-empty]");
  const searchViewToggles = Array.from(root.querySelectorAll("[data-search-view-toggle]"));

  let activeCountry = "";
  let locationMatchCountry = "";
  let searchIndex = null;
  let searchIndexUnavailable = false;
  let searchResultView = "grouped";

  const pluralize = (count, singular, plural = `${singular}s`) =>
    `${count} ${count === 1 ? singular : plural}`;

  const normalizeCountry = (value) => String(value || "").trim().toLowerCase();

  const countryLabel = (country) => {
    if (!country) {
      return "";
    }

    const button = countryButtons.find((candidate) => (candidate.dataset.country || "") === country);
    const label = button?.querySelector("span")?.textContent || country;
    return label.replace(FLAG_SUFFIX_PATTERN, "").trim();
  };

  const guideLocations = (() => {
    try {
      return JSON.parse(homeGuideMap?.dataset.guides || "[]").filter(
        (guide) =>
          guide &&
          typeof guide.country === "string" &&
          typeof guide.countryName === "string" &&
          typeof guide.lat === "number" &&
          typeof guide.lng === "number",
      );
    } catch {
      return [];
    }
  })();

  const setLocationStatus = (message, tone = "") => {
    if (!locationStatus) {
      return;
    }

    locationStatus.textContent = message;
    locationStatus.dataset.tone = tone;
  };

  const setLocationButtonBusy = (busy) => {
    if (!locationButton) {
      return;
    }

    locationButton.disabled = busy;
    locationButton.dataset.busy = busy ? "true" : "false";
    locationButton.setAttribute("aria-busy", busy ? "true" : "false");
  };

  const toRadians = (degrees) => (degrees * Math.PI) / 180;

  const distanceInKm = (fromLat, fromLng, toLat, toLng) => {
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
  };

  const nearestGuideForLocation = (latitude, longitude) =>
    guideLocations.reduce((nearest, guide) => {
      const distance = distanceInKm(latitude, longitude, guide.lat, guide.lng);

      if (!nearest || distance < nearest.distance) {
        return { ...guide, distance };
      }

      return nearest;
    }, null);

  const createGuideLink = (entry, query) => {
    const params = new URLSearchParams();
    params.set("place", entry.id);
    if (query) {
      params.set("q", query);
    }
    return `/guides/${entry.guide_slug}/?${params.toString()}`;
  };

  const truncateText = (value, maxLength) => {
    if (!value || value.length <= maxLength) {
      return value || "";
    }
    return `${value.slice(0, maxLength - 1).trim()}...`;
  };

  const groupSearchResults = (results) =>
    [...results.reduce((groups, result) => {
      const country = result.entry.country || "Unknown";
      if (!groups.has(country)) {
        groups.set(country, []);
      }
      groups.get(country).push(result);
      return groups;
    }, new Map()).entries()]
      .map(([country, items]) => ({ country, items }))
      .sort(
        (left, right) =>
          right.items.length - left.items.length ||
          right.items[0].score - left.items[0].score ||
          left.country.localeCompare(right.country),
      );

  const updateSearchViewButtons = () => {
    searchViewToggles.forEach((toggle) => {
      const selected = (toggle.dataset.searchView || "") === searchResultView;
      toggle.dataset.active = selected ? "true" : "false";
      toggle.setAttribute("aria-pressed", selected ? "true" : "false");
    });
  };

  const createSearchResultCard = (result, query) => {
    const entry = result.entry;
    const card = document.createElement("article");
    card.className = "search-result-card";

    const title = document.createElement("h4");
    title.textContent = entry.name || "Saved place";

    const meta = document.createElement("p");
    meta.className = "meta-copy";
    meta.textContent = [
      entry.guide_title,
      [entry.category, entry.neighborhood].filter(Boolean).join(" · "),
      [entry.city, entry.country].filter(Boolean).join(", "),
    ]
      .filter(Boolean)
      .join(" · ");

    const copy = document.createElement("p");
    copy.className = "small-copy";
    copy.textContent = truncateText(entry.why_recommended || entry.note || "", 180);
    copy.hidden = !copy.textContent;

    const tags = document.createElement("div");
    tags.className = "tag-row";
    [...(entry.vibe_tags || []), ...(entry.tags || [])].slice(0, 5).forEach((tag) => {
      const pill = document.createElement("span");
      pill.className = "tag-pill";
      pill.textContent = tag.includes("-") ? tag.replaceAll("-", " ") : `#${tag}`;
      tags.appendChild(pill);
    });
    tags.hidden = tags.childElementCount === 0;

    const link = document.createElement("a");
    link.className = "action-pill";
    link.href = createGuideLink(entry, query);
    link.textContent = "Open in guide";

    card.append(title, meta, copy, tags, link);
    return card;
  };

  const createGroupedCountryCard = (group, query) => {
    const card = document.createElement("article");
    card.className = "search-country-card";

    const header = document.createElement("div");
    header.className = "search-country-card-head";

    const title = document.createElement("h4");
    title.textContent = group.country;

    const count = document.createElement("p");
    count.className = "small-copy";
    count.textContent =
      group.items.length > GROUP_LIMIT
        ? `${pluralize(group.items.length, "match")} · top ${GROUP_LIMIT} shown`
        : pluralize(group.items.length, "match");

    header.append(title, count);

    const list = document.createElement("div");
    list.className = "search-country-list";

    group.items.slice(0, GROUP_LIMIT).forEach((result) => {
      const entry = result.entry;
      const link = document.createElement("a");
      link.className = "search-country-item";
      link.href = createGuideLink(entry, query);

      const itemTitle = document.createElement("span");
      itemTitle.className = "search-country-item-title";
      itemTitle.textContent = entry.name || "Saved place";

      const itemMeta = document.createElement("span");
      itemMeta.className = "search-country-item-meta";
      itemMeta.textContent = [
        entry.guide_title,
        [entry.category, entry.neighborhood].filter(Boolean).join(" · "),
        entry.city,
      ]
        .filter(Boolean)
        .join(" · ");

      link.append(itemTitle, itemMeta);
      list.append(link);
    });

    card.append(header, list);

    if (group.items.length > GROUP_LIMIT) {
      const overflow = document.createElement("p");
      overflow.className = "search-country-overflow";
      overflow.textContent = `${group.items.length - GROUP_LIMIT} more matching place${
        group.items.length - GROUP_LIMIT === 1 ? "" : "s"
      }. Switch to individual view to browse more.`;
      card.append(overflow);
    }

    return card;
  };

  const dispatchMapUpdate = (visibleGuideSlugs, visibleGuideCount, query) => {
    document.dispatchEvent(
      new CustomEvent("favorite-places:home-map-update", {
        detail: {
          activeCountry: countryLabel(activeCountry),
          query,
          visibleGuideCount,
          visibleGuideSlugs,
        },
      }),
    );
  };

  const renderGlobalSearch = (query) => {
    if (!globalResults || !globalResultsList || !groupedResultsList || !globalResultsEmpty) {
      return;
    }

    globalResults.hidden = !query;
    globalResults.setAttribute("aria-hidden", query ? "false" : "true");

    if (!query) {
      globalResultsList.replaceChildren();
      groupedResultsList.replaceChildren();
      globalResultsList.hidden = true;
      groupedResultsList.hidden = false;
      globalResultsEmpty.dataset.visible = "false";
      if (globalResultsToolbar) {
        globalResultsToolbar.hidden = false;
      }
      if (globalResultsTitle) {
        globalResultsTitle.textContent = "Matching places";
      }
      if (globalResultsSummary) {
        globalResultsSummary.textContent = "";
      }
      updateSearchViewButtons();
      return;
    }

    if (searchIndexUnavailable) {
      globalResultsList.replaceChildren();
      groupedResultsList.replaceChildren();
      globalResultsList.hidden = true;
      groupedResultsList.hidden = true;
      globalResultsEmpty.dataset.visible = "true";
      globalResultsEmpty.textContent = "Place search is unavailable. Guide browsing still works.";
      if (globalResultsToolbar) {
        globalResultsToolbar.hidden = true;
      }
      if (globalResultsTitle) {
        globalResultsTitle.textContent = "Place search unavailable";
      }
      if (globalResultsSummary) {
        globalResultsSummary.textContent = "";
      }
      return;
    }

    if (!searchIndex) {
      globalResultsList.replaceChildren();
      groupedResultsList.replaceChildren();
      globalResultsEmpty.dataset.visible = "false";
      if (globalResultsToolbar) {
        globalResultsToolbar.hidden = true;
      }
      if (globalResultsTitle) {
        globalResultsTitle.textContent = "Loading matching places";
      }
      if (globalResultsSummary) {
        globalResultsSummary.textContent = "Loading index...";
      }
      return;
    }

    const state = searchPlaces(query, { index: searchIndex, scope: "all" });
    const filteredResults = state.results.filter(
      (result) => !activeCountry || normalizeCountry(result.entry.country) === activeCountry,
    );
    const groupedResults = groupSearchResults(filteredResults);
    const visibleIndividualResults = filteredResults.slice(0, INDIVIDUAL_RESULT_LIMIT);

    globalResultsList.replaceChildren(...visibleIndividualResults.map((result) => createSearchResultCard(result, query)));
    groupedResultsList.replaceChildren(...groupedResults.map((group) => createGroupedCountryCard(group, query)));

    globalResultsList.hidden = searchResultView !== "individual";
    groupedResultsList.hidden = searchResultView !== "grouped";
    globalResultsEmpty.dataset.visible = filteredResults.length === 0 ? "true" : "false";
    globalResultsEmpty.textContent = activeCountry
      ? `No matching places in ${countryLabel(activeCountry)}. Try a broader search.`
      : "No matching places. Try a broader search.";

    if (globalResultsToolbar) {
      globalResultsToolbar.hidden = false;
    }

    if (globalResultsTitle) {
      globalResultsTitle.textContent =
        searchResultView === "grouped"
          ? `${pluralize(filteredResults.length, "matching place")} across ${pluralize(
              groupedResults.length,
              "country",
              "countries",
            )}`
          : `${pluralize(filteredResults.length, "matching place")}`;
    }

    if (globalResultsSummary) {
      const parsed = [
        ...state.parsed.vibes.map((vibe) => vibe.replaceAll("-", " ")),
        ...state.parsed.categories,
      ];
      const summaryBits = [];

      if (parsed.length > 0) {
        summaryBits.push(parsed.join(" · "));
      }

      if (activeCountry) {
        summaryBits.push(`Filtered to ${countryLabel(activeCountry)}`);
      }

      if (searchResultView === "grouped" && filteredResults.length > 0) {
        summaryBits.push(`Showing up to ${GROUP_LIMIT} places per country`);
      } else if (searchResultView === "individual" && filteredResults.length > INDIVIDUAL_RESULT_LIMIT) {
        summaryBits.push(`Showing top ${INDIVIDUAL_RESULT_LIMIT} individual matches`);
      }

      globalResultsSummary.textContent = summaryBits.join(" · ") || "All guides";
    }

    updateSearchViewButtons();
  };

  const updateCountryButtons = (matchingGuidesByCountry, totalMatchingGuides) => {
    countryButtons.forEach((button) => {
      const country = button.dataset.country || "";
      const count = country ? matchingGuidesByCountry.get(country) || 0 : totalMatchingGuides;
      const countLabel = button.querySelector(".country-filter-count");

      button.dataset.active = country === activeCountry ? "true" : "false";
      button.dataset.locationMatch = country && country === locationMatchCountry ? "true" : "false";
      button.setAttribute("aria-pressed", country === activeCountry ? "true" : "false");
      button.disabled = Boolean(country) && country !== activeCountry && count === 0;

      if (countLabel) {
        countLabel.textContent = String(count);
      }
    });
  };

  const update = () => {
    const query = (searchInput?.value || "").trim().toLowerCase();
    const guideMatches =
      searchIndex && query
        ? new Set(searchGuides(query, { index: searchIndex }).map((result) => result.guide.slug))
        : null;
    const matchingGuidesByCountry = new Map();
    const visibleGuideSlugs = [];
    let visibleGuideCount = 0;
    let visibleCountryCount = 0;
    let totalMatchingGuides = 0;

    countryBlocks.forEach((block) => {
      const country = block.dataset.country || "";
      const cards = Array.from(block.querySelectorAll("[data-guide-card]"));
      const matchingCards = cards.filter((card) => {
        if (!query) {
          return true;
        }
        return (card.dataset.search || "").includes(query) || guideMatches?.has(card.dataset.guideSlug || "");
      });
      const matchingCardSet = new Set(matchingCards);

      block.dataset.locationMatch = country && country === locationMatchCountry ? "true" : "false";
      matchingGuidesByCountry.set(country, matchingCards.length);
      totalMatchingGuides += matchingCards.length;

      const selectedCountryMatches = !activeCountry || country === activeCountry;
      let blockVisibleGuideCount = 0;

      cards.forEach((card) => {
        const visible = selectedCountryMatches && matchingCardSet.has(card);
        card.hidden = !visible;

        if (visible) {
          blockVisibleGuideCount += 1;
          if (card.dataset.guideSlug) {
            visibleGuideSlugs.push(card.dataset.guideSlug);
          }
        }
      });

      block.hidden = blockVisibleGuideCount === 0;

      if (blockVisibleGuideCount > 0) {
        visibleCountryCount += 1;
        visibleGuideCount += blockVisibleGuideCount;
      }
    });

    updateCountryButtons(matchingGuidesByCountry, totalMatchingGuides);

    if (resultsCount) {
      resultsCount.textContent = `${pluralize(visibleGuideCount, "guide")} across ${pluralize(
        visibleCountryCount,
        "country",
        "countries",
      )}`;
    }

    if (emptyState) {
      emptyState.dataset.visible = visibleGuideCount === 0 ? "true" : "false";
    }

    dispatchMapUpdate([...new Set(visibleGuideSlugs)], visibleGuideCount, query);
    renderGlobalSearch(query);
  };

  const selectCountry = (country, { fromLocation = false, scroll = false } = {}) => {
    activeCountry = country;

    if (fromLocation) {
      locationMatchCountry = country;
      if (searchInput) {
        searchInput.value = "";
      }
    } else {
      locationMatchCountry = "";
      setLocationStatus("");
    }

    update();

    if (scroll) {
      const button = countryButtons.find((candidate) => (candidate.dataset.country || "") === country);
      const block = countryBlocks.find((candidate) => (candidate.dataset.country || "") === country);

      button?.scrollIntoView({ behavior: "smooth", block: "nearest", inline: "center" });
      block?.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  };

  const setSearchView = (view) => {
    const nextView = view === "individual" ? "individual" : "grouped";
    if (searchResultView === nextView) {
      return;
    }

    searchResultView = nextView;
    renderGlobalSearch((searchInput?.value || "").trim().toLowerCase());
  };

  searchInput?.addEventListener("input", update);

  countryButtons.forEach((button) => {
    button.addEventListener("click", () => {
      selectCountry(button.dataset.country || "");
    });
  });

  searchViewToggles.forEach((toggle) => {
    toggle.addEventListener("click", () => {
      setSearchView(toggle.dataset.searchView || "grouped");
    });
  });

  if (locationButton) {
    if (!("geolocation" in navigator) || guideLocations.length === 0) {
      locationButton.disabled = true;
      setLocationStatus("Location is not available.", "error");
    } else {
      locationButton.addEventListener("click", () => {
        setLocationButtonBusy(true);
        setLocationStatus("Locating...", "pending");

        navigator.geolocation.getCurrentPosition(
          (position) => {
            const nearestGuide = nearestGuideForLocation(
              position.coords.latitude,
              position.coords.longitude,
            );

            setLocationButtonBusy(false);

            if (!nearestGuide) {
              setLocationStatus("No guide countries have map data.", "error");
              return;
            }

            selectCountry(nearestGuide.country, { fromLocation: true, scroll: true });
            setLocationStatus(`Showing nearby guides in ${nearestGuide.countryName}.`, "success");
          },
          (error) => {
            setLocationButtonBusy(false);

            if (error.code === error.PERMISSION_DENIED) {
              setLocationStatus("Location permission was not granted.", "error");
              return;
            }

            if (error.code === error.TIMEOUT) {
              setLocationStatus("Location timed out. Try again.", "error");
              return;
            }

            setLocationStatus("Could not get your location.", "error");
          },
          {
            enableHighAccuracy: false,
            maximumAge: 10 * 60 * 1000,
            timeout: 10000,
          },
        );
      });
    }
  }

  updateSearchViewButtons();
  update();

  loadSearchIndex()
    .then((index) => {
      searchIndex = index;
      update();
    })
    .catch(() => {
      searchIndexUnavailable = true;
      update();
    });
}
