import { defineConfig } from "@playwright/test";

const e2eDir = new URL("..", import.meta.url).pathname;

export default defineConfig({
  testDir: e2eDir,
  testMatch: "*.spec.ts",
  timeout: 30_000,
  retries: 0,
  workers: 1,

  use: {
    baseURL: "http://localhost:5176",
    browserName: "chromium",
  },

  webServer: {
    command: "npx vite --config fixture/vite.config.ts",
    port: 5176,
    reuseExistingServer: !process.env.CI,
    cwd: e2eDir,
  },
});
