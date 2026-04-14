import type { SiteConfig } from "./site";

export const siteConfig: SiteConfig = {
  siteName: "Favorite Places",
  ownerName: "Your Name",
  siteDescription:
    "Shareable travel guides built from your own public Google Maps saved lists.",
  defaultSeoImage: "/social-card.png",
  home: {
    eyebrow: "Favorite Places",
    title: "Saved places, but actually readable.",
    intro:
      "Start from public Google Maps saved lists, then layer on your own curation, tags, and top picks.",
    highlightsHeading: "Why use this site",
    highlights: [
      "Top picks surface first instead of hiding inside a giant list.",
      "Country and list tags make guides browseable before search.",
      "Each place card can hand off directly into Google Maps.",
    ],
  },
  guide: {
    fallbackDescription:
      "A curated saved-places guide with quick search, tags, and direct Google Maps handoff.",
  },
};
