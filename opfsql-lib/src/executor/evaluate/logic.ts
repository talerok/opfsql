import type {
  BoundBetweenExpression,
  BoundComparisonExpression,
  BoundConjunctionExpression,
  ComparisonType,
} from "../../sql/binder/types.js";
import {
  and,
  compareSql,
  or,
  type Truth,
  truthOf,
  type Value,
} from "../../values/index.js";
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
  const test = compare(expression.comparisonType);
  return (row) => test(left(row), right(row));
}

export function compileConjunction(
  expression: BoundConjunctionExpression,
  scope: Scope,
): CompiledExpression {
  const children = expression.children.map((child) => scope.compile(child));
  // AND stops at the first false, OR at the first true: the result is
  // decided, and the remaining operands are not evaluated.
  const [combine, decisive] =
    expression.conjunctionType === "AND" ? [and, false] : [or, true];
  return (row) => {
    let result: Truth = !decisive;
    for (const child of children) {
      result = combine(result, truthOf(child(row)));
      if (result === decisive) return result;
    }
    return result;
  };
}

/** `x BETWEEN a AND b` is `x >= a AND x <= b`, with x evaluated once. */
export function compileBetween(
  expression: BoundBetweenExpression,
  scope: Scope,
): CompiledExpression {
  const input = scope.compile(expression.input);
  const lower = scope.compile(expression.lower);
  const upper = scope.compile(expression.upper);
  const atLeast = compare("GREATER_EQUAL");
  const atMost = compare("LESS_EQUAL");
  return (row) => {
    const value = input(row);
    return and(atLeast(value, lower(row)), atMost(value, upper(row)));
  };
}
