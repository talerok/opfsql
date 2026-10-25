// Benchmarks in a real browser: a worker opens each database with Engine.open
// on the browser's OPFS, so the timings include real disk I/O.
import { chromium, firefox, webkit } from "@playwright/test";
import { fileURLToPath } from "node:url";
import { createServer } from "vite";
import { parseCli, report } from "./cli.js";

const options = parseCli(process.argv.slice(2));
const browserType = { chromium, firefox, webkit }[options.browser];

const server = await createServer({
  configFile: false,
  root: fileURLToPath(new URL("./browser", import.meta.url)),
  logLevel: "error",
  server: {
    port: 0,
    fs: { allow: [fileURLToPath(new URL("..", import.meta.url))] },
  },
});
await server.listen();

const browser = await browserType.launch();
try {
  const page = await browser.newPage();
  await page.goto(server.resolvedUrls!.local[0]);
  const results = await page.evaluate(
    (benchOptions) => window.runBenchmarks(benchOptions),
    {
      repeats: options.repeats,
      warmup: options.warmup,
      filter: options.filter,
    },
  );
  report(results, options);
} finally {
  await browser.close();
  await server.close();
}
