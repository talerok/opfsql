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
          setupFiles: ["test/setup/web-locks.ts"],
          provide: { backend: "wal" },
        },
      },
    ],
  },
});
