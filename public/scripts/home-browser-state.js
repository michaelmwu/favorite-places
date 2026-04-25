const DEFAULT_VIEW = "grouped";

const normalizeCountry = (value) =>
  String(value || "")
    .trim()
    .toLowerCase();

export function slugifyCountry(country) {
  return normalizeCountry(country)
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/&/g, " and ")
    .replace(/['']/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function resolveCountry(country, validCountries = []) {
  const normalizedCountry = normalizeCountry(country);
  if (!normalizedCountry) {
    return "";
  }

  if (!Array.isArray(validCountries) || validCountries.length === 0) {
    return normalizedCountry;
  }

  for (const candidate of validCountries) {
    const normalizedCandidate = normalizeCountry(candidate);
    if (!normalizedCandidate) {
      continue;
    }

    if (
      normalizedCountry === normalizedCandidate ||
      normalizedCountry === slugifyCountry(candidate)
    ) {
      return normalizedCandidate;
    }
  }

  return "";
}

export function parseHomeBrowserHash(hash, validCountries = []) {
  const rawHash = String(hash || "")
    .replace(/^#/, "")
    .trim();
  if (!rawHash) {
    return {
      country: "",
      query: "",
      view: DEFAULT_VIEW,
    };
  }

  if (!/[=&]/.test(rawHash)) {
    return {
      country: resolveCountry(decodeURIComponent(rawHash), validCountries),
      query: "",
      view: DEFAULT_VIEW,
    };
  }

  const params = new URLSearchParams(rawHash.startsWith("?") ? rawHash.slice(1) : rawHash);

  return {
    country: resolveCountry(params.get("country") || "", validCountries),
    query: String(params.get("q") || "").trim(),
    view: params.get("view") === "individual" ? "individual" : DEFAULT_VIEW,
  };
}

export function serializeHomeBrowserHash({ country = "", query = "", view = DEFAULT_VIEW } = {}) {
  const normalizedCountry = normalizeCountry(country);
  const trimmedQuery = String(query || "").trim();
  const normalizedView = view === "individual" ? "individual" : DEFAULT_VIEW;

  if (!normalizedCountry && !trimmedQuery && normalizedView === DEFAULT_VIEW) {
    return "";
  }

  if (normalizedCountry && !trimmedQuery && normalizedView === DEFAULT_VIEW) {
    return `#${encodeURIComponent(slugifyCountry(normalizedCountry))}`;
  }

  const params = new URLSearchParams();

  if (normalizedCountry) {
    params.set("country", slugifyCountry(normalizedCountry));
  }

  if (trimmedQuery) {
    params.set("q", trimmedQuery);
  }

  if (normalizedView !== DEFAULT_VIEW) {
    params.set("view", normalizedView);
  }

  const serialized = params.toString();
  return serialized ? `#${serialized}` : "";
}
