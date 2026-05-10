import { describe, expect, it, vi } from "vitest";

describe("site config merging", () => {
  it("preserves legacy top-level guideCard config", async () => {
    vi.resetModules();
    vi.doMock("favorite-places:site-config", () => ({
      siteConfig: {
        guideCard: {
          showDescription: false,
          maxTags: 2,
          trimCountryFromTitle: false,
        },
      },
    }));
    const { siteConfig } = await import("../../src/data/site");
    vi.doUnmock("favorite-places:site-config");

    expect(siteConfig.home.guideCard.showDescription).toBe(false);
    expect(siteConfig.home.guideCard.maxTags).toBe(2);
    expect(siteConfig.home.guideCard.trimCountryFromTitle).toBe(false);
    expect("guideCard" in siteConfig).toBe(false);
  });

  it("lets nested home guideCard config override legacy config", async () => {
    vi.resetModules();
    vi.doMock("favorite-places:site-config", () => ({
      siteConfig: {
        guideCard: {
          showDescription: false,
          maxTags: 2,
        },
        home: {
          guideCard: {
            showDescription: true,
            maxTags: 6,
          },
        },
      },
    }));
    const { siteConfig } = await import("../../src/data/site");
    vi.doUnmock("favorite-places:site-config");

    expect(siteConfig.home.guideCard.showDescription).toBe(true);
    expect(siteConfig.home.guideCard.maxTags).toBe(6);
  });

  it("merges place attribution config with defaults", async () => {
    vi.resetModules();
    vi.doMock("favorite-places:site-config", () => ({
      siteConfig: {
        placeCard: {
          showAttribution: false,
        },
      },
    }));
    const { siteConfig } = await import("../../src/data/site");
    vi.doUnmock("favorite-places:site-config");

    expect(siteConfig.placeCard.showAttribution).toBe(false);
    expect(siteConfig.placeCard.showGuideAuthorAttribution).toBe(false);
    expect(siteConfig.placeCard.attributionLabel).toBe("Added by");
  });
});
