import { fileURLToPath } from "node:url";
import { configDefaults, defineConfig } from "vitest/config";

export default defineConfig({
  resolve: {
    alias: {
      "favorite-places:site-config": fileURLToPath(
        new URL("./site.example/config.ts", import.meta.url),
      ),
    },
  },
  test: {
    exclude: [...configDefaults.exclude, ".context/**", "tests/e2e/**"],
  },
});
