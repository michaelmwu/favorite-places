import manifests from "../data/generated/manifests.json";
import type { Guide, GuideManifest } from "./types";

const guideModules = import.meta.glob("../data/generated/lists/*.json", {
  eager: true,
});

function isGuideModule(value: unknown): value is { default: Guide } {
  return typeof value === "object" && value !== null && "default" in value;
}

export function getGuideManifests(): GuideManifest[] {
  return [...manifests].sort((a, b) => a.title.localeCompare(b.title));
}

export function getGuides(): Guide[] {
  return Object.values(guideModules)
    .filter(isGuideModule)
    .map((module) => module.default)
    .sort((a, b) => a.title.localeCompare(b.title));
}

export function getGuideBySlug(slug: string): Guide | undefined {
  return getGuides().find((guide) => guide.slug === slug);
}
