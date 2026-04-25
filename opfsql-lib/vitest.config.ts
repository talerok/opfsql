import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    exclude: [
      "**/node_modules/**",
      "**/store-old/**",
      "**/executor-old/**",
      "**/engine-old/**",
      "test/e2e/**",
    ],
  },
});
