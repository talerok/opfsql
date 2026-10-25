import type {
  BoundExpression,
  BoundOperatorExpression,
  LogicalType,
} from "../../binder/types.js";
import {
  foldTruth,
  membership,
  type Truth,
  type Value,
} from "../../../values/index.js";
import type { Tuple } from "../types.js";
import { affinityOf, comparisonConversion } from "./affinity.js";
import { compareTyped } from "./logic.js";
import { fixedValue, required } from "./parameter.js";
import type { CompiledExpression, Scope } from "./scope.js";

// ---------------------------------------------------------------------------
// `x IN (items)`: true if some item equals x, as `=` compares — comparison
// affinity included, so the answer does not depend on whether the
// optimizer rewrote a short list into `=`s. A list of constants is hashed
// once; each row then costs one lookup.
// ---------------------------------------------------------------------------

export function compileInList(
  expression: BoundOperatorExpression,
  scope: Scope,
): CompiledExpression {
  const [left, ...items] = expression.children;
  const needleExpression = required(left, "the left side of IN");
  const needle = scope.compile(needleExpression);
  const type = needleExpression.returnType;
  return (
    compileConstantList(needle, type, items, scope) ??
    compileItems(needle, type, items, scope)
  );
}

/** Each item compared with x by `=`, the answers combined by OR. */
function compileItems(
  needle: CompiledExpression,
  needleType: LogicalType,
  items: readonly BoundExpression[],
  scope: Scope,
): CompiledExpression {
  const tests = items.map((item) => ({
    value: scope.compile(item),
    equals: compareTyped("EQUAL", needleType, item.returnType),
  }));
  function* comparisons(row: Tuple, value: Value): Generator<Truth> {
    for (const test of tests) yield test.equals(value, test.value(row));
  }
  return (row) => foldTruth("or", comparisons(row, needle(row)));
}

/**
 * A list of constants and parameters, hashed once. x of unknown type is
 * converted to the items' affinity, so they must all share one.
 */
function compileConstantList(
  needle: CompiledExpression,
  needleType: LogicalType,
  items: readonly BoundExpression[],
  scope: Scope,
): CompiledExpression | null {
  const [first] = items;
  if (first === undefined) return null;
  const affinities = new Set(items.map((item) => affinityOf(item.returnType)));
  const hasMixedAffinity = needleType === "ANY" && affinities.size > 1;
  const values = items.map((item) => fixedValue(item, scope.context));
  if (hasMixedAffinity || values.includes(undefined)) return null;
  const converted = items.map((item, index) =>
    comparisonConversion(item.returnType, needleType)(values[index] ?? null),
  );
  const convertNeedle = comparisonConversion(needleType, first.returnType);
  const contains = membership(converted);
  return (row) => contains(convertNeedle(needle(row)));
}
