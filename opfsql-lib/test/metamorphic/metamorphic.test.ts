import { describe, expect, it } from "vitest";
import { runSeed } from "./run.js";

// ---------------------------------------------------------------------------
// Metamorphic tests of the whole pipeline — parse, bind, optimize, execute —
// against the engine itself, as SQLancer tests a database: random databases
// and well-typed random queries, and oracles that hold forms of one query
// to each other (oracles.ts); then random changes, which the copy with
// indexes must make as the copy without does (changes.ts).
//
// A failure names its seed and case; METAMORPHIC_SEED=<seed> runs from that
// seed. METAMORPHIC_SEEDS=<n> runs n databases: a few in every test run,
// thousands for a soak before changing the planner, which prints how many
// checks it ran and skipped.
// ---------------------------------------------------------------------------

const FIRST_SEED = Number(process.env["METAMORPHIC_SEED"] ?? 1);
const IS_SOAK = process.env["METAMORPHIC_SEEDS"] !== undefined;
const SEEDS = Number(process.env["METAMORPHIC_SEEDS"] ?? 4);
const CASES_PER_SEED = 20;
/** Only catches a run that hangs: a busy machine may take many times longer. */
const TIMEOUT = Math.max(60_000, SEEDS * 5000);

describe("metamorphic oracles", () => {
  it(
    "find no query whose forms disagree",
    async () => {
      const failures: string[] = [];
      let checks = 0;
      let parted = 0;
      const skipped = new Map<string, number>();
      for (let seed = FIRST_SEED; seed < FIRST_SEED + SEEDS; seed++) {
        const report = await runSeed(seed, CASES_PER_SEED);
        failures.push(...report.failures);
        checks += report.checks;
        if (report.hasParted) parted++;
        for (const [code, count] of report.skipped) {
          skipped.set(code, (skipped.get(code) ?? 0) + count);
        }
        // Let the test runner's own messages through on a long run.
        await new Promise((resolve) => setTimeout(resolve, 0));
      }
      if (IS_SOAK) {
        process.stdout.write(`${summary(checks, skipped, parted)}\n`);
      }
      expect(failures.join("\n\n")).toBe("");
    },
    TIMEOUT,
  );
});

/** What a soak checked: the figure to quote for it. */
function summary(
  checks: number,
  skipped: ReadonlyMap<string, number>,
  parted: number,
): string {
  const skips = [...skipped].map(([code, count]) => `${code} ${String(count)}`);
  return [
    `metamorphic: seeds ${String(FIRST_SEED)}–${String(FIRST_SEED + SEEDS - 1)},`,
    `${String(checks)} checks,`,
    `skipped: ${skips.length === 0 ? "none" : skips.join(", ")};`,
    `changes stopped where the copies parted: ${String(parted)} databases`,
  ].join(" ");
}
