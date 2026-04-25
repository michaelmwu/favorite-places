import { cpSync, createReadStream, existsSync, statSync } from "node:fs";
import { extname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "astro/config";

const ROOT = fileURLToPath(new URL(".", import.meta.url));

function resolveSiteDir() {
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

function resolveSiteFile(siteDir, relativePath, fallbackPath) {
  const sitePath = join(siteDir, relativePath);
  return existsSync(sitePath) ? sitePath : fallbackPath;
}

function contentTypeFor(pathname) {
  switch (extname(pathname)) {
    case ".css":
      return "text/css; charset=utf-8";
    case ".ico":
      return "image/x-icon";
    case ".jpg":
    case ".jpeg":
      return "image/jpeg";
    case ".json":
      return "application/json; charset=utf-8";
    case ".cjs":
    case ".js":
    case ".mjs":
      return "text/javascript; charset=utf-8";
    case ".png":
      return "image/png";
    case ".svg":
      return "image/svg+xml; charset=utf-8";
    case ".webp":
      return "image/webp";
    default:
      return "application/octet-stream";
  }
}

function sitePublicIntegration(sitePublicDir) {
  return {
    name: "favorite-places-site-public",
    hooks: {
      "astro:server:setup": ({ server }) => {
        if (!existsSync(sitePublicDir)) {
          return;
        }

        server.middlewares.use((request, response, next) => {
          if (request.method !== "GET" && request.method !== "HEAD") {
            next();
            return;
          }

          const pathname = decodeURIComponent(
            new URL(request.url ?? "/", "http://localhost").pathname,
          );
          const publicPath = resolve(sitePublicDir, `.${pathname}`);
          const relativePublicPath = relative(sitePublicDir, publicPath);
          if (relativePublicPath.startsWith("..")) {
            next();
            return;
          }

          try {
            if (!statSync(publicPath).isFile()) {
              next();
              return;
            }
          } catch {
            next();
            return;
          }

          response.setHeader("Content-Type", contentTypeFor(publicPath));
          createReadStream(publicPath).pipe(response);
        });
      },
      "astro:build:done": ({ dir }) => {
        if (!existsSync(sitePublicDir)) {
          return;
        }

        cpSync(sitePublicDir, fileURLToPath(dir), {
          recursive: true,
          force: true,
        });
      },
    },
  };
}

const siteDir = resolveSiteDir();
const siteConfigPath = resolveSiteFile(
  siteDir,
  "config.ts",
  join(ROOT, "src", "data", "site.default.ts"),
);
const siteThemePath = resolveSiteFile(
  siteDir,
  "theme.css",
  join(ROOT, "src", "styles", "empty.css"),
);

export default defineConfig({
  output: "static",
  site: "https://favorite-places.pages.dev",
  integrations: [sitePublicIntegration(join(siteDir, "public"))],
  vite: {
    plugins: [tailwindcss()],
    resolve: {
      alias: {
        "favorite-places:site-config": siteConfigPath,
        "favorite-places:site-theme": siteThemePath,
      },
    },
    server: {
      fs: {
        allow: [ROOT, siteDir],
      },
    },
  },
});
