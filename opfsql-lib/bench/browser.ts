// Benchmarks in a real browser: a worker opens each database with Engine.open
// on the browser's OPFS, so the timings include real disk I/O.
import { chromium, firefox, webkit } from "@playwright/test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createServer } from "vite";
import { parseCli, report, type CliOptions } from "./cli.js";
import type { BenchOptions, ScenarioResult } from "./harness.js";

declare global {
  // The benchmark page's entry point (bench/browser/main.ts).
  var runBenchmarks: (options: BenchOptions) => Promise<ScenarioResult[]>;
}

/** Serves bench/browser and runs the scenarios in a page of `options.browser`. */
async function runInBrowser(options: CliOptions): Promise<ScenarioResult[]> {
  const server = await createServer({
    configFile: false,
    root: fileURLToPath(new URL("browser", import.meta.url)),
    logLevel: "error",
    server: {
      port: 0,
      fs: { allow: [fileURLToPath(new URL("..", import.meta.url))] },
    },
  });
  await server.listen();
  // A persistent profile, as a user's. An ephemeral context is private
  // browsing, where Chromium's OPFS takes about 100 µs a file operation
  // instead of about 1, and WebKit has no OPFS at all.
  const profile = await mkdtemp(path.join(tmpdir(), "opfsql-bench-"));
  const browserType = { chromium, firefox, webkit }[options.browser];
  const context = await browserType.launchPersistentContext(profile);
  try {
    const page = await context.newPage();
    await page.goto(server.resolvedUrls!.local[0]);
    return await page.evaluate(
      async (benchOptions) => globalThis.runBenchmarks(benchOptions),
      {
        repeats: options.repeats,
        warmup: options.warmup,
        filter: options.filter,
      },
    );
  } finally {
    await context.close();
    await server.close();
    await rm(profile, { recursive: true, force: true });
  }
}

const options = parseCli(process.argv.slice(2));
if (options !== undefined) report(await runInBrowser(options), options);
