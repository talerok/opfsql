import { expect } from "vitest";
import type { Value } from "../../../values/index.js";
import type { logical } from "../../plan/index.js";
import { bindKey, optimizedPlan } from "./builders.js";
import { Random, sampleTables } from "./random.js";
import { bothRuns, bothSequences } from "./reference.js";

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

/**
 * As expectSameRows, and in the same order: for a plan ordered by keys that
 * tell every row apart, so that one order is right.
 */
export function expectSameSequence(
  plan: logical.Plan,
  parameters: readonly Value[] = [],
): void {
  const optimized = optimizedPlan(plan);
  for (const tables of SAMPLES) {
    const data = { tables, parameters };
    const { expected, actual } = bothSequences(plan, optimized, data, bindKey);
    expect(actual).toEqual(expected);
  }
}
