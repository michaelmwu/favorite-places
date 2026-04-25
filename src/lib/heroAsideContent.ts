import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

import { siteDir } from "./sitePaths";

function readTemplate(relativePath: string): string | null {
  const templatePath = join(siteDir, "content", "templates", relativePath);
  if (!existsSync(templatePath)) {
    return null;
  }

  return readFileSync(templatePath, "utf-8");
}

export function getTemplateHtml(relativePath: string): string | null {
  return readTemplate(relativePath);
}

export function getHomeAsideHtml(): string | null {
  return readTemplate("home-aside.html");
}

export function getGuideAsideHtml(slug: string): string | null {
  return readTemplate(`guide-aside/${slug}.html`) ?? readTemplate("guide-aside/default.html");
}
