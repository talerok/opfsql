import {
  opfsHost,
  runScenarios,
  type BenchOptions,
  type ScenarioResult,
} from "../harness.js";
import { SCENARIOS } from "../scenarios.js";

export type WorkerReply = { results: ScenarioResult[] } | { error: string };

self.onmessage = async ({ data }: MessageEvent<BenchOptions>) => {
  let reply: WorkerReply;
  try {
    reply = { results: await runScenarios(SCENARIOS, opfsHost, data) };
  } catch (err) {
    reply = {
      error: err instanceof Error ? (err.stack ?? err.message) : String(err),
    };
  }
  postMessage(reply);
};
