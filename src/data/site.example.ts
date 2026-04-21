import type { SiteConfig } from "./site";

export const siteConfig: SiteConfig = {
  siteName: "DEM Flyers Places",
  ownerName: "Your Name",
  siteDescription: "City bookmarks organized for trip planning.",
  defaultSeoImage: "/social-card.png",
  favicon: {
    href: "/favicon.png",
    type: "image/png",
    sizes: "32x32",
  },
  demFlyersUrl: "https://www.demflyers.com/",
  home: {
    eyebrow: "DEM Flyers",
    title: "Trip suggestions",
    intro: "Saved restaurants, cafes, bars, shops, and sights by city.",
    highlightsHeading: "Also on DEM Flyers",
    highlights: [
      "See [beacons.ai/demflyers](https://beacons.ai/demflyers) for more, or follow on [Instagram](https://www.instagram.com/demflyers/), [Threads](https://www.threads.net/@demflyers), or [BlueSky](https://bsky.app/profile/demflyers.bsky.social) as @demflyers.",
    ],
  },
  guide: {
    fallbackDescription: "Saved places for this city.",
    sidebarContent: [
      "See [beacons.ai/demflyers](https://beacons.ai/demflyers) for more, or follow on [Instagram](https://www.instagram.com/demflyers/), [Threads](https://www.threads.net/@demflyers), or [BlueSky](https://bsky.app/profile/demflyers.bsky.social) as @demflyers.",
    ],
  },
};
