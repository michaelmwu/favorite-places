/// <reference types="astro/client" />

interface ImportMetaEnv {
  readonly PUBLIC_MAP_PROVIDER?: string;
  readonly PUBLIC_PLACE_PHOTOS?: string;
}

interface ProcessEnv {
  FAVORITE_PLACES_SITE_DIR?: string;
  GOOGLE_MAPS_JS_API_KEY?: string;
}

declare const process: {
  env: ProcessEnv;
  cwd(): string;
};

interface ImportMeta {
  readonly env: ImportMetaEnv;
}

declare module "favorite-places:site-config" {
  import type { SiteConfigInput } from "./data/site";

  export const siteConfig: SiteConfigInput;
}

declare module "favorite-places:site-theme" {}

declare module "node:fs" {
  export function existsSync(path: string): boolean;
  export function readFileSync(path: string, encoding: string): string;
}

declare module "node:path" {
  export function join(...paths: string[]): string;
  export function resolve(...paths: string[]): string;
}
