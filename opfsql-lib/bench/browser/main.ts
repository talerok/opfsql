import type { BenchOptions, ScenarioResult } from "../harness.js";
import type { WorkerReply } from "./worker.js";

declare global {
  interface Window {
    /** Called by bench/browser.ts through Playwright. */
    runBenchmarks(options: BenchOptions): Promise<ScenarioResult[]>;
  }
}

// OPFS sync access handles exist only in workers, so the engine runs in one.
window.runBenchmarks = (options) =>
  new Promise((resolve, reject) => {
    const worker = new Worker(new URL("./worker.ts", import.meta.url), {
      type: "module",
    });
    worker.onmessage = ({ data }: MessageEvent<WorkerReply>) => {
      worker.terminate();
      if ("error" in data) reject(new Error(data.error));
      else resolve(data.results);
    };
    worker.onerror = (event) => {
      worker.terminate();
      reject(new Error(event.message));
    };
    worker.postMessage(options);
  });
