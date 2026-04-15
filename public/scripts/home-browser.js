const root = document.querySelector("[data-home-browser-root]");

if (root) {
  const searchInput = root.querySelector("[data-home-search-input]");
  const countryBlocks = Array.from(root.querySelectorAll("[data-country-block]"));
  const countryButtons = Array.from(root.querySelectorAll("[data-country-filter]"));
  const resultsCount = root.querySelector("[data-home-results-count]");
  const emptyState = root.querySelector("[data-home-empty-state]");

  let activeCountry = "";

  const pluralize = (count, singular, plural = `${singular}s`) =>
    `${count} ${count === 1 ? singular : plural}`;

  const updateCountryButtons = (matchingGuidesByCountry, totalMatchingGuides) => {
    countryButtons.forEach((button) => {
      const country = button.dataset.country || "";
      const count = country ? matchingGuidesByCountry.get(country) || 0 : totalMatchingGuides;
      const countLabel = button.querySelector(".country-filter-count");

      button.dataset.active = country === activeCountry ? "true" : "false";
      button.setAttribute("aria-pressed", country === activeCountry ? "true" : "false");
      button.disabled = Boolean(country) && country !== activeCountry && count === 0;

      if (countLabel) {
        countLabel.textContent = String(count);
      }
    });
  };

  const update = () => {
    const query = (searchInput?.value || "").trim().toLowerCase();
    const matchingGuidesByCountry = new Map();
    let visibleGuideCount = 0;
    let visibleCountryCount = 0;
    let totalMatchingGuides = 0;

    countryBlocks.forEach((block) => {
      const country = block.dataset.country || "";
      const cards = Array.from(block.querySelectorAll("[data-guide-card]"));
      const matchingCards = cards.filter((card) => !query || (card.dataset.search || "").includes(query));
      const matchingCardSet = new Set(matchingCards);

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
  };

  searchInput?.addEventListener("input", update);

  countryButtons.forEach((button) => {
    button.addEventListener("click", () => {
      activeCountry = button.dataset.country || "";
      update();
    });
  });

  update();
}
