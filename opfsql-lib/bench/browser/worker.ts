import {
  opfsHost,
  runScenarios,
  type BenchOptions,
  type ScenarioResult,
} from "../harness.js";
import { SCENARIOS } from "../scenarios.js";

export type WorkerReply = { results: ScenarioResult[] } | { error: string };

/**
 * This dedicated worker's global scope, which the DOM types call a Window:
 * its messages come only from the page that started it.
 */
interface WorkerScope {
  postMessage(reply: WorkerReply): void;
  addEventListener(
    type: "message",
    listener: (event: MessageEvent<BenchOptions>) => void,
  ): void;
}

const scope: WorkerScope = globalThis;

/** Runs the scenarios `options` asks for and posts the results back. */
async function answer(options: BenchOptions): Promise<void> {
  let reply: WorkerReply;
  try {
    reply = { results: await runScenarios(SCENARIOS, opfsHost, options) };
  } catch (error) {
    reply = { error: describe(error) };
  }
  scope.postMessage(reply);
}

/** An error's stack, which starts with its message, or the thrown value. */
function describe(error: unknown): string {
  if (!(error instanceof Error)) return String(error);
  return error.stack ?? error.message;
}

scope.addEventListener("message", (event) => {
  void answer(event.data);
});
