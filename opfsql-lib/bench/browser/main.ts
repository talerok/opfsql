import type { BenchOptions, ScenarioResult } from "../harness.js";
import type { WorkerReply } from "./worker.js";

/**
 * Runs the benchmarks in a worker of their own: OPFS sync access handles
 * exist only in workers.
 */
async function runBenchmarks(options: BenchOptions): Promise<ScenarioResult[]> {
  const worker = new Worker(new URL("worker.ts", import.meta.url), {
    type: "module",
  });
  const reply = Promise.withResolvers<ScenarioResult[]>();
  worker.addEventListener("message", ({ data }: MessageEvent<WorkerReply>) => {
    if ("error" in data) reply.reject(new Error(data.error));
    else reply.resolve(data.results);
  });
  worker.addEventListener("error", (event) => {
    reply.reject(new Error(event.message));
  });
  worker.postMessage(options);
  try {
    return await reply.promise;
  } finally {
    worker.terminate();
  }
}

// bench/browser.ts calls it through Playwright.
Object.assign(globalThis, { runBenchmarks });
