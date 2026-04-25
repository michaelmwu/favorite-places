import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "astro/config";

export default defineConfig({
  output: "static",
  site: "https://favorite-places.pages.dev",
  vite: {
    plugins: [tailwindcss()],
  },
});
