import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    exclude: ["**/node_modules/**", "test/e2e/**"],
    setupFiles: ["test/setup/web-locks.ts"],
    projects: [
      {
        extends: true,
        test: { name: "default", provide: { pages: "default" } },
      },
      {
        // Engine-level suites again with 4 KB pages and frequent
        // checkpoints, so ordinary test data splits B-trees, spills into
        // overflow pages and is checkpointed.
        extends: true,
        test: {
          name: "small-pages",
          include: ["src/engine/tests/**/*.test.ts", "test/slt/**/*.test.ts"],
          // These choose their own page size and threshold.
          exclude: [
            "src/engine/tests/integrity.test.ts",
            "src/engine/tests/multi-tab.test.ts",
            "src/engine/tests/open.test.ts",
            "src/engine/tests/snapshot.test.ts",
            "src/engine/tests/storage-efficiency.test.ts",
          ],
          provide: { pages: "small" },
        },
      },
    ],
  },
});
