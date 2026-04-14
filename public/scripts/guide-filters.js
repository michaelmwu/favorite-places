const root = document.querySelector("[data-guide-root]");

if (root) {
  const cards = Array.from(root.querySelectorAll("[data-place-card]"));
  const list = root.querySelector("[data-place-list]");
  const searchInput = root.querySelector("[data-search-input]");
  const sortSelect = root.querySelector("[data-sort-select]");
  const tagButtons = Array.from(root.querySelectorAll("[data-tag-filter]"));
  const resultsCount = root.querySelector("[data-results-count]");
  const emptyState = root.querySelector("[data-empty-state]");

  let activeTag = "";

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

  const update = () => {
    const query = (searchInput?.value || "").trim().toLowerCase();
    const sort = sortSelect?.value || "curated";

    const visibleCards = cards.filter((card) => {
      const matchesQuery = !query || (card.dataset.search || "").includes(query);
      const matchesTag = !activeTag || (card.dataset.tags || "").split(" ").includes(activeTag);
      const visible = matchesQuery && matchesTag;
      card.hidden = !visible;
      return visible;
    });

    visibleCards.sort(sorters[sort] || sorters.curated).forEach((card) => {
      list?.appendChild(card);
    });

    if (resultsCount) {
      resultsCount.textContent = `${visibleCards.length} place${visibleCards.length === 1 ? "" : "s"}`;
    }

    if (emptyState) {
      emptyState.dataset.visible = visibleCards.length === 0 ? "true" : "false";
    }
  };

  searchInput?.addEventListener("input", update);
  sortSelect?.addEventListener("change", update);
  tagButtons.forEach((button) => {
    button.addEventListener("click", () => {
      activeTag = button.dataset.tag || "";
      tagButtons.forEach((candidate) => {
        candidate.dataset.active = candidate === button ? "true" : "false";
      });
      update();
    });
  });

  const allButton = tagButtons.find((button) => (button.dataset.tag || "") === "");
  if (allButton) {
    allButton.dataset.active = "true";
  }
  update();
}
