const asideTemplateModules = import.meta.glob("/src/content/templates/**/*.html", {
  eager: true,
  import: "default",
  query: "?raw",
}) as Record<string, string>;

export function getHomeAsideHtml(): string | null {
  return asideTemplateModules["/src/content/templates/home-aside.html"] ?? null;
}

export function getGuideAsideHtml(slug: string): string | null {
  return (
    asideTemplateModules[`/src/content/templates/guide-aside/${slug}.html`] ??
    asideTemplateModules["/src/content/templates/guide-aside/default.html"] ??
    null
  );
}
