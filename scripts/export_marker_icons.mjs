import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  buildMapMarkerSvg,
  getMapMarkerColors,
  getMapMarkerLabel,
  MAP_MARKER_ICON_NAMES,
} from "../src/lib/mapMarkerIcons";

const currentDir = dirname(fileURLToPath(import.meta.url));
const rootDir = resolve(currentDir, "..");
const outputDir = resolve(rootDir, "public", "marker-icons");

mkdirSync(outputDir, { recursive: true });

const manifest = MAP_MARKER_ICON_NAMES.map((icon) => {
  const filename = `${icon}.svg`;
  const svg = `${buildMapMarkerSvg(icon, getMapMarkerColors(icon))}\n`;

  writeFileSync(resolve(outputDir, filename), svg, "utf8");

  return {
    file: filename,
    key: icon,
    label: getMapMarkerLabel(icon),
  };
});

writeFileSync(resolve(outputDir, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`, "utf8");

const previewHtml = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Favorite Places Marker Icons</title>
    <style>
      :root {
        color-scheme: light;
        --bg: #f7efe3;
        --card: rgba(255, 250, 241, 0.92);
        --line: rgba(39, 26, 22, 0.12);
        --ink: #1f1a16;
        --muted: #6e5e53;
      }

      * {
        box-sizing: border-box;
      }

      body {
        margin: 0;
        font-family: "Avenir Next", "Segoe UI", sans-serif;
        background:
          radial-gradient(circle at top, rgba(196, 166, 133, 0.28), transparent 38%),
          linear-gradient(180deg, #fbf6ef 0%, var(--bg) 100%);
        color: var(--ink);
      }

      main {
        width: min(72rem, calc(100% - 2rem));
        margin: 0 auto;
        padding: 2rem 0 3rem;
      }

      h1 {
        margin: 0;
        font-size: clamp(1.8rem, 5vw, 3rem);
        line-height: 1.05;
      }

      p {
        max-width: 42rem;
        color: var(--muted);
        font-size: 1rem;
        line-height: 1.6;
      }

      .grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(10rem, 1fr));
        gap: 1rem;
        margin-top: 1.5rem;
      }

      .card {
        display: grid;
        gap: 0.75rem;
        justify-items: center;
        padding: 1.1rem 1rem 1rem;
        border: 1px solid var(--line);
        border-radius: 1.1rem;
        background: var(--card);
        box-shadow: 0 14px 40px rgba(31, 26, 22, 0.06);
        text-align: center;
      }

      .card img {
        width: 54px;
        height: 72px;
      }

      .card strong {
        font-size: 0.96rem;
      }

      .card code {
        color: var(--muted);
        font-size: 0.8rem;
      }
    </style>
  </head>
  <body>
    <main>
      <h1>Marker Icons</h1>
      <p>Static SVG exports generated from the same marker source used by the guide map. Grab any file directly from this folder or use the manifest for programmatic lookups.</p>
      <section class="grid">
        ${manifest
          .map(
            (entry) => `
              <article class="card">
                <img src="./${entry.file}" alt="${entry.label}" />
                <strong>${entry.label}</strong>
                <code>${entry.file}</code>
              </article>
            `.trim(),
          )
          .join("\n        ")}
      </section>
    </main>
  </body>
</html>
`;

writeFileSync(resolve(outputDir, "index.html"), `${previewHtml}\n`, "utf8");

console.log(`Exported ${manifest.length} marker icons to ${outputDir}`);
