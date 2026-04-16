/// <reference types="astro/client" />

interface ImportMetaEnv {
  readonly PUBLIC_MAP_PROVIDER?: string;
}

interface ProcessEnv {
  GOOGLE_MAPS_JS_API_KEY?: string;
}

declare const process: {
  env: ProcessEnv;
};

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
