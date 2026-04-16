import { loadSearchIndex, searchGuides, searchPlaces } from "./place-search.js";

const root = document.querySelector("[data-home-browser-root]");

if (root) {
  const searchInput = root.querySelector("[data-home-search-input]");
  const countryBlocks = Array.from(root.querySelectorAll("[data-country-block]"));
  const countryButtons = Array.from(root.querySelectorAll("[data-country-filter]"));
  const resultsCount = root.querySelector("[data-home-results-count]");
  const emptyState = root.querySelector("[data-home-empty-state]");
  const locationButton = root.querySelector("[data-location-target]");
  const locationStatus = root.querySelector("[data-location-status]");
  const locationGuideIndex = document.querySelector("[data-location-guide-index]");
  const globalResults = root.querySelector("[data-global-search-results]");
  const globalResultsTitle = root.querySelector("[data-global-search-title]");
  const globalResultsSummary = root.querySelector("[data-global-search-summary]");
  const globalResultsList = root.querySelector("[data-global-search-list]");
  const globalResultsEmpty = root.querySelector("[data-global-search-empty]");

  let activeCountry = "";
  let locationMatchCountry = "";
  let searchIndex = null;
  let searchIndexUnavailable = false;

  const pluralize = (count, singular, plural = `${singular}s`) =>
    `${count} ${count === 1 ? singular : plural}`;

  const guideLocations = (() => {
    try {
      return JSON.parse(locationGuideIndex?.textContent || "[]").filter(
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
    const guideMatches = searchIndex && query ? new Set(
      searchGuides(query, { index: searchIndex }).map((result) => result.guide.slug),
    ) : null;
    const matchingGuidesByCountry = new Map();
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

    renderGlobalSearch(query);
  };

  const renderGlobalSearch = (query) => {
    if (!globalResults || !globalResultsList || !globalResultsEmpty) {
      return;
    }

    globalResults.hidden = !query;
    globalResults.setAttribute("aria-hidden", query ? "false" : "true");
    if (!query) {
      globalResultsList.replaceChildren();
      globalResultsEmpty.dataset.visible = "false";
      if (globalResultsTitle) {
        globalResultsTitle.textContent = "Matching places";
      }
      if (globalResultsSummary) {
        globalResultsSummary.textContent = "";
      }
      return;
    }

    if (searchIndexUnavailable) {
      globalResultsList.replaceChildren();
      globalResultsEmpty.dataset.visible = "true";
      globalResultsEmpty.textContent = "Place search is unavailable. Guide browsing still works.";
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
      globalResultsEmpty.dataset.visible = "false";
      if (globalResultsTitle) {
        globalResultsTitle.textContent = "Loading matching places";
      }
      if (globalResultsSummary) {
        globalResultsSummary.textContent = "Loading index...";
      }
      return;
    }

    const state = searchPlaces(query, { index: searchIndex, scope: "all" });
    const results = state.results.slice(0, 24);
    globalResultsList.replaceChildren(...results.map((result) => createSearchResultCard(result, query)));

    if (globalResultsTitle) {
      globalResultsTitle.textContent = `${state.count} matching place${state.count === 1 ? "" : "s"}`;
    }
    if (globalResultsSummary) {
      const parsed = [
        ...state.parsed.vibes.map((vibe) => vibe.replaceAll("-", " ")),
        ...state.parsed.categories,
      ];
      globalResultsSummary.textContent = parsed.length > 0 ? parsed.join(" · ") : "All guides";
    }
    globalResultsEmpty.dataset.visible = state.count === 0 ? "true" : "false";
    globalResultsEmpty.textContent = "No matching places. Try a broader search.";
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
    ].filter(Boolean).join(" · ");

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
    const params = new URLSearchParams();
    params.set("place", entry.id);
    if (query) {
      params.set("q", query);
    }
    link.href = `/guides/${entry.guide_slug}/?${params.toString()}`;
    link.textContent = "Open in guide";

    card.append(title, meta, copy, tags, link);
    return card;
  };

  const truncateText = (value, maxLength) => {
    if (!value || value.length <= maxLength) {
      return value || "";
    }
    return `${value.slice(0, maxLength - 1).trim()}...`;
  };

  searchInput?.addEventListener("input", update);

  countryButtons.forEach((button) => {
    button.addEventListener("click", () => {
      selectCountry(button.dataset.country || "");
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
