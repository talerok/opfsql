import { expect } from "vitest";
import type { Value } from "../../../values/index.js";
import type { logical } from "../../plan/index.js";
import { bindKey, optimizedPlan } from "./builders.js";
import { Random, sampleTables } from "./random.js";
import { bothRuns } from "./reference.js";

// Checks a plan the optimizer made against the naive plan of the same
// logical plan, over several sample tables: the same rows, whatever the
// access path, keys or order chosen.

const SAMPLES = [1, 2, 3, 4].map((seed) => sampleTables(new Random(seed)));

export function expectSameRows(
  plan: logical.Plan,
  parameters: readonly Value[] = [],
): void {
  const optimized = optimizedPlan(plan);
  for (const tables of SAMPLES) {
    const { expected, actual } = bothRuns(
      plan,
      optimized,
      { tables, parameters },
      bindKey,
    );
    expect(actual).toEqual(expected);
  }
}
