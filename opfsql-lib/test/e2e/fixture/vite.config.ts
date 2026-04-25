import { defineConfig } from "vite";

const __dirname = new URL(".", import.meta.url).pathname;
const root = new URL("../../..", import.meta.url).pathname;

export default defineConfig({
  root: __dirname,
  server: {
    port: 5176,
    fs: {
      allow: [root],
    },
    headers: {
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Embedder-Policy": "require-corp",
    },
  },
});
