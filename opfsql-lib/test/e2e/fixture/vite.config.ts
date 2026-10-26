import { defineConfig } from "vite";

const here = new URL(".", import.meta.url).pathname;
const root = new URL("../../..", import.meta.url).pathname;

export default defineConfig({
  root: here,
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
