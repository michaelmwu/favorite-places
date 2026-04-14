export interface SiteConfig {
  siteName: string;
  ownerName: string;
  siteDescription: string;
  defaultSeoImage: string | null;
  home: {
    eyebrow: string;
    title: string;
    intro: string;
    highlightsHeading: string;
    highlights: string[];
  };
  guide: {
    fallbackDescription: string;
  };
}

export const siteConfig: SiteConfig = {
  siteName: "Favorite Places",
  ownerName: "Michael Wu",
  siteDescription:
    "A personal collection of saved-place guides built for sharing, searching, and quick Google Maps handoff.",
  defaultSeoImage: null,
  home: {
    eyebrow: "Favorite Places",
    title: "Saved places, but actually readable.",
    intro:
      "These guides start from public Google Maps saved lists, then add curation, tags, top picks, and much faster scanning on mobile. The actual save and navigation flow still belongs to Google Maps. The presentation layer does not.",
    highlightsHeading: "What makes this better",
    highlights: [
      "Top picks surface first instead of disappearing inside a giant list.",
      "List tags and country grouping make the site browseable before search.",
      "Every place card hands off directly into Google Maps in one tap.",
    ],
  },
  guide: {
    fallbackDescription:
      "A personal saved-places guide with quick search, tags, and a direct Google Maps handoff.",
  },
};
