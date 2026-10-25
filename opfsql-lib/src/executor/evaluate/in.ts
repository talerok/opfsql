import {
  type BoundExpression,
  BoundExpressionClass,
  type BoundOperatorExpression,
  type LogicalType,
} from "../../sql/binder/types.js";
import { membership, or, type Truth, type Value } from "../../values/index.js";
import { affinityOf, comparisonConversion } from "./affinity.js";
import { compareTyped } from "./logic.js";
import { parameterValue } from "./parameter.js";
import type { CompiledExpression, Scope } from "./scope.js";

// ---------------------------------------------------------------------------
// `x IN (items)`: true if some item equals x, as `=` compares — comparison
// affinity included, so the answer does not depend on whether the
// optimizer rewrote a short list into `=`s. A list of constants is hashed
// once; each row then costs one lookup.
// ---------------------------------------------------------------------------

export function compileIn(
  expression: BoundOperatorExpression,
  scope: Scope,
): CompiledExpression {
  const [needleExpression, ...items] = expression.children;
  if (needleExpression === undefined) return () => false;
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
  return (row) => {
    const value = needle(row);
    let found: Truth = false;
    for (const test of tests) {
      found = or(found, test.equals(value, test.value(row)));
      if (found === true) return true;
    }
    return found;
  };
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
  const affinities = new Set(items.map((item) => affinityOf(item.returnType)));
  const [first] = items;
  if (first === undefined || (needleType === "ANY" && affinities.size > 1)) {
    return null;
  }
  const values: Value[] = [];
  for (const item of items) {
    const value = fixedValue(item, scope);
    if (value === undefined) return null;
    values.push(comparisonConversion(item.returnType, needleType)(value));
  }
  const convertNeedle = comparisonConversion(needleType, first.returnType);
  const contains = membership(values);
  return (row) => contains(convertNeedle(needle(row)));
}

/** The value of an item that is the same for every row, if it is. */
function fixedValue(item: BoundExpression, scope: Scope): Value | undefined {
  if (item.expressionClass === BoundExpressionClass.BOUND_CONSTANT) {
    return item.value;
  }
  return item.expressionClass === BoundExpressionClass.BOUND_PARAMETER
    ? parameterValue(scope.context, item.index)
    : undefined;
}
