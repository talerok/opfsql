import { describe, expect, it } from "vitest";
import { runSeed } from "./run.js";

// ---------------------------------------------------------------------------
// Metamorphic tests of the whole pipeline — parse, bind, optimize, execute —
// against the engine itself, as SQLancer tests a database: random databases
// and well-typed random queries, and oracles that hold forms of one query
// to each other (oracles.ts).
//
// A failure names its seed and case; METAMORPHIC_SEED=<seed> runs from that
// seed. METAMORPHIC_SEEDS=<n> runs n databases: a few in every test run,
// thousands for a soak before changing the planner.
// ---------------------------------------------------------------------------

const FIRST_SEED = Number(process.env["METAMORPHIC_SEED"] ?? 1);
const SEEDS = Number(process.env["METAMORPHIC_SEEDS"] ?? 6);
const CASES_PER_SEED = 25;
/** Only catches a run that hangs: a busy machine may take many times longer. */
const TIMEOUT = Math.max(60_000, SEEDS * 5000);

describe("metamorphic oracles", () => {
  it(
    "find no query whose forms disagree",
    async () => {
      const failures: string[] = [];
      for (let seed = FIRST_SEED; seed < FIRST_SEED + SEEDS; seed++) {
        const report = await runSeed(seed, CASES_PER_SEED);
        failures.push(...report.failures);
        // Let the test runner's own messages through on a long run.
        await new Promise((resolve) => setTimeout(resolve, 0));
      }
      expect(failures.join("\n\n")).toBe("");
    },
    TIMEOUT,
  );
});
