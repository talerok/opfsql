import { setFlagsFromString } from "node:v8";
import { runInNewContext } from "node:vm";

/**
 * Collects garbage and lets finalization callbacks run; they are queued as
 * tasks after a collection, so a few turns of the event loop pass between.
 * V8 exposes gc() to contexts created after the flag is set.
 */
export async function collectGarbage(): Promise<void> {
  setFlagsFromString("--expose-gc");
  const gc = runInNewContext("gc") as () => void;
  for (let round = 0; round < 5; round++) {
    gc();
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
}
