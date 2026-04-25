import { existsSync } from "node:fs";
import { resolve } from "node:path";

const ROOT = process.cwd();

export function resolveSiteDir(): string {
  const configuredSiteDir = process.env.FAVORITE_PLACES_SITE_DIR;
  if (configuredSiteDir) {
    return resolve(ROOT, configuredSiteDir);
  }

  const defaultSiteDir = resolve(ROOT, "site");
  if (existsSync(defaultSiteDir)) {
    return defaultSiteDir;
  }

  return resolve(ROOT, "site.example");
}

export const siteDir = resolveSiteDir();
