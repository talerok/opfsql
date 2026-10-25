import type {
  BoundBetweenExpression,
  BoundComparisonExpression,
  BoundConjunctionExpression,
  ComparisonType,
  LogicalType,
} from "../../binder/types.js";
import {
  and,
  compareSql,
  foldTruth,
  type Truth,
  truthOf,
  type Value,
} from "../../../values/index.js";
import type { Tuple } from "../types.js";
import { comparisonConversion } from "./affinity.js";
import type { CompiledExpression, Scope } from "./scope.js";

// ---------------------------------------------------------------------------
// Comparisons and three-valued logic, all from values: compareSql for the
// order, and/or/not for NULL as "unknown".
// ---------------------------------------------------------------------------

/** `order` (as compareValues returns it) satisfies `comparison`. */
export function comparisonHolds(
  comparison: ComparisonType,
): (order: number) => boolean {
  switch (comparison) {
    case "EQUAL": {
      return (order) => order === 0;
    }
    case "NOT_EQUAL": {
      return (order) => order !== 0;
    }
    case "LESS": {
      return (order) => order < 0;
    }
    case "LESS_EQUAL": {
      return (order) => order <= 0;
    }
    case "GREATER": {
      return (order) => order > 0;
    }
    case "GREATER_EQUAL": {
      return (order) => order >= 0;
    }
  }
}

/** `left <comparison> right`: NULL when either side is NULL. */
export function compare(
  comparison: ComparisonType,
): (left: Value, right: Value) => Truth {
  const holds = comparisonHolds(comparison);
  return (left, right) => {
    const order = compareSql(left, right);
    return order === null ? null : holds(order);
  };
}

export function compileComparison(
  expression: BoundComparisonExpression,
  scope: Scope,
): CompiledExpression {
  const left = scope.compile(expression.left);
  const right = scope.compile(expression.right);
  const test = compareTyped(
    expression.comparisonType,
    expression.left.returnType,
    expression.right.returnType,
  );
  return (row) => test(left(row), right(row));
}

/**
 * `compare`, with comparison affinity for operands of types `left` and
 * `right`: a side of unknown type is converted to the other side's type.
 */
export function compareTyped(
  comparison: ComparisonType,
  left: LogicalType,
  right: LogicalType,
): (left: Value, right: Value) => Truth {
  const test = compare(comparison);
  const convertLeft = comparisonConversion(left, right);
  const convertRight = comparisonConversion(right, left);
  return (a, b) => test(convertLeft(a), convertRight(b));
}

export function compileConjunction(
  expression: BoundConjunctionExpression,
  scope: Scope,
): CompiledExpression {
  const children = expression.children.map((child) => scope.compile(child));
  const kind = expression.conjunctionType === "AND" ? "and" : "or";
  // foldTruth stops once the answer is decided, so the generator never
  // evaluates the remaining operands.
  function* truths(row: Tuple): Generator<Truth> {
    for (const child of children) yield truthOf(child(row));
  }
  return (row) => foldTruth(kind, truths(row));
}

/** `x BETWEEN a AND b` is `x >= a AND x <= b`, with x evaluated once. */
export function compileBetween(
  expression: BoundBetweenExpression,
  scope: Scope,
): CompiledExpression {
  const input = scope.compile(expression.input);
  const lower = scope.compile(expression.lower);
  const upper = scope.compile(expression.upper);
  const type = expression.input.returnType;
  const atLeast = compareTyped(
    "GREATER_EQUAL",
    type,
    expression.lower.returnType,
  );
  const atMost = compareTyped("LESS_EQUAL", type, expression.upper.returnType);
  return (row) => {
    const value = input(row);
    return and(atLeast(value, lower(row)), atMost(value, upper(row)));
  };
}
