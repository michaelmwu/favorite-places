export interface SiteConfig {
  siteName: string;
  ownerName: string;
  siteDescription: string;
  defaultSeoImage: string | null;
  favicon: {
    href: string;
    type?: string;
    sizes?: string;
  } | null;
  demFlyersUrl: string;
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
  siteName: "DEM Flyers Places",
  ownerName: "Michael Wu",
  siteDescription: "Michael's city bookmarks from DEM Flyers, organized for quick trip planning.",
  defaultSeoImage: null,
  favicon: {
    href: "/demflyers-favicon.png",
    type: "image/png",
    sizes: "32x32",
  },
  demFlyersUrl: "https://www.demflyers.com/",
  home: {
    eyebrow: "DEM Flyers",
    title: "Trip suggestions",
    intro: "Michael's saved restaurants, cafes, bars, shops, and sights by city.",
    highlightsHeading: "Also on DEM Flyers",
    highlights: [
      "Posts, trip reports, and travel notes live on the main site.",
      "Google Maps and Wanderlog lists are linked where available.",
      "Country sections below mirror the trip suggestions index.",
    ],
  },
  guide: {
    fallbackDescription: "Michael's saved places for this city.",
  },
};
