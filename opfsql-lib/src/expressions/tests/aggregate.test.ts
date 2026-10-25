import { describe, expect, it } from "vitest";
import { aggregateFunction, resolveCall } from "../../functions/index.js";
import type { AggregateCall, physical } from "../../sql/plan/index.js";
import type { Value } from "../../values/index.js";
import { compileAggregate, type Tuple } from "../index.js";
import { arithmetic, constant } from "./builders.js";
import { c0, scopeOf } from "./harness.js";

function aggregate(
  name: string,
  operands: readonly physical.Expression[],
  isDistinct = false,
): AggregateCall<physical.Plan> {
  const definition = aggregateFunction(name);
  const types = operands.map((operand) => operand.type);
  const type = resolveCall(definition, types);
  return { name, function: definition, arguments: operands, isDistinct, type };
}

/** The rows aggregated, as one group: t0.c0 is 1, 2, 2, NULL. */
const ROWS: Tuple[] = [[1], [2], [2], [null]];

/** The aggregate's value over `ROWS`. */
function overRows(call: AggregateCall<physical.Plan>): Value {
  const compiled = compileAggregate(call, scopeOf());
  const accumulator = compiled.start();
  for (const row of ROWS) accumulator.add(compiled.argumentsOf(row));
  return accumulator.result();
}

describe("compileAggregate", () => {
  it("counts rows for COUNT(*)", () => {
    expect(overRows(aggregate("count", []))).toBe(4);
  });

  it("skips rows with a NULL argument", () => {
    expect(overRows(aggregate("count", [c0]))).toBe(3);
  });

  it("skips arguments seen before for DISTINCT", () => {
    expect(overRows(aggregate("count", [c0], true))).toBe(2);
    expect(overRows(aggregate("sum", [c0], true))).toBe(3);
  });

  it("evaluates its arguments for each row", () => {
    const tenfold = arithmetic(c0, "*", constant(10));
    expect(overRows(aggregate("sum", [tenfold]))).toBe(50);
  });

  it("starts each group empty", () => {
    const compiled = compileAggregate(aggregate("count", [c0]), scopeOf());
    const first = compiled.start();
    first.add(compiled.argumentsOf([1]));
    expect(compiled.start().result()).toBe(0);
    expect(first.result()).toBe(1);
  });

  it("reads arguments into one array, reused for every row", () => {
    const compiled = compileAggregate(aggregate("sum", [c0]), scopeOf());
    expect(compiled.argumentsOf([1])).toBe(compiled.argumentsOf([2]));
  });
});
