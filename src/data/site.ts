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
    sidebarContent: string[];
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
      "See [beacons.ai/demflyers](https://beacons.ai/demflyers) for more, or follow on [Instagram](https://www.instagram.com/demflyers/), [Threads](https://www.threads.net/@demflyers), or [BlueSky](https://bsky.app/profile/demflyers.bsky.social) as @demflyers.",
    ],
  },
  guide: {
    fallbackDescription: "Michael's saved places for this city.",
    sidebarContent: [
      "See [beacons.ai/demflyers](https://beacons.ai/demflyers) for more, or follow on [Instagram](https://www.instagram.com/demflyers/), [Threads](https://www.threads.net/@demflyers), or [BlueSky](https://bsky.app/profile/demflyers.bsky.social) as @demflyers.",
    ],
  },
};
