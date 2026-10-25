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
    setupFiles: ["test/setup/web-locks.ts"],
    projects: [
      {
        extends: true,
        test: { name: "opfs", provide: { backend: "opfs" } },
      },
      {
        // Engine-level suites again, on the production storage stack.
        extends: true,
        test: {
          name: "wal",
          include: ["src/engine/tests/**/*.test.ts", "test/slt/**/*.test.ts"],
          // These build their own storage stack, so a second run adds nothing.
          exclude: [
            "src/engine/tests/explain-format.test.ts",
            "src/engine/tests/integrity.test.ts",
            "src/engine/tests/multi-tab.test.ts",
            "src/engine/tests/storage-efficiency.test.ts",
          ],
          provide: { backend: "wal" },
        },
      },
    ],
  },
});
