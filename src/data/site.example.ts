import type { SiteConfig } from "./site";

export const siteConfig: SiteConfig = {
  siteName: "DEM Flyers Places",
  ownerName: "Your Name",
  siteDescription: "City bookmarks organized for trip planning.",
  defaultSeoImage: "/social-card.png",
  demFlyersUrl: "https://www.demflyers.com/",
  home: {
    eyebrow: "DEM Flyers",
    title: "Trip suggestions",
    intro: "Saved restaurants, cafes, bars, shops, and sights by city.",
    highlightsHeading: "Also on DEM Flyers",
    highlights: [
      "Posts, trip reports, and travel notes live on the main site.",
      "Google Maps and Wanderlog lists are linked where available.",
      "Country sections mirror the trip suggestions index.",
    ],
  },
  guide: {
    fallbackDescription: "Saved places for this city.",
  },
};
