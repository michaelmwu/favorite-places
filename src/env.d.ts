/// <reference types="astro/client" />

interface ImportMetaEnv {
  readonly GOOGLE_MAPS_JS_API_KEY?: string;
  readonly PUBLIC_MAP_PROVIDER?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
