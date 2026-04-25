export const siteConfig = {
  siteName: "DEM Flyers Places",
  ownerName: "Michael Wu",
  siteDescription: "Michael's city bookmarks from DEM Flyers, organized for quick trip planning.",
  defaultSeoImage: null,
  favicon: {
    href: "/demflyers-favicon.png",
    type: "image/png",
    sizes: "32x32",
  },
  logo: {
    href: "https://www.demflyers.com/",
    src: "/demflyers-logo.png",
    alt: "DEM Flyers",
  },
  navLinks: [
    { label: "Places", href: "/" },
    { label: "Main site", href: "https://www.demflyers.com/" },
  ],
  mapProvider: "auto",
  home: {
    eyebrow: "DEM Flyers",
    title: "Trip suggestions",
    intro: "Michael's saved restaurants, cafes, bars, shops, and sights by city.",
    highlightsHeading: "Also on DEM Flyers",
    highlights: [
      "See [beacons.ai/demflyers](https://beacons.ai/demflyers) for more, or follow on [Instagram](https://www.instagram.com/demflyers/), [Threads](https://www.threads.net/@demflyers), or [BlueSky](https://bsky.app/profile/demflyers.bsky.social) as @demflyers.",
    ],
    guidesLinkText: "More travel notes at DEM Flyers",
  },
  guide: {
    fallbackDescription: "Michael's saved places for this city.",
    sidebarContent: [
      "See [beacons.ai/demflyers](https://beacons.ai/demflyers) for more, or follow on [Instagram](https://www.instagram.com/demflyers/), [Threads](https://www.threads.net/@demflyers), or [BlueSky](https://bsky.app/profile/demflyers.bsky.social) as @demflyers.",
    ],
    topPicksEyebrow: "DEM Flyers",
    topPicksHeading: "Top suggestions",
  },
  guideCard: {
    fallbackDescription: "Saved places from Michael's travel notes.",
  },
};
