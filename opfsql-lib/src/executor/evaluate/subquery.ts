import type { BoundSubqueryExpression } from "../../sql/binder/types.js";
import { foldTruth, type Truth, type Value } from "../../values/index.js";
import { ExecutorError } from "../errors.js";
import type { Tuple } from "../types.js";
import { compareTyped } from "./logic.js";
import { required } from "./parameter.js";
import type { CompiledExpression, Scope } from "./scope.js";

// ---------------------------------------------------------------------------
// Subqueries in expressions. Each evaluation runs the subquery's plan for
// the current row, which correlated subqueries read as their outer row.
// ---------------------------------------------------------------------------

export function compileSubquery(
  expression: BoundSubqueryExpression,
  scope: Scope,
): CompiledExpression {
  const { resolver, context } = scope;
  const plan = expression.subplan;
  const run = (row: Tuple, limit?: number): Tuple[] =>
    context.runSubquery(plan, { tuple: row, resolver }, limit);
  switch (expression.subqueryType) {
    case "EXISTS": {
      return (row) => run(row, 1).length > 0;
    }
    case "NOT_EXISTS": {
      return (row) => run(row, 1).length === 0;
    }
    case "SCALAR": {
      return (row) => scalar(run(row, 2));
    }
    case "ANY":
    case "ALL": {
      return compileQuantified(expression, scope, run);
    }
  }
}

function scalar(rows: readonly Tuple[]): Value {
  if (rows.length > 1) {
    throw new ExecutorError("Scalar subquery returned more than one row");
  }
  return rows[0]?.[0] ?? null;
}

/**
 * `x <op> ANY (subquery)` is true if the comparison holds for some row,
 * `ALL` if it holds for every row; three-valued: a NULL comparison makes
 * the answer NULL unless another row decides it. No rows: ANY is false
 * and ALL is true, whatever `x` is — so `NULL IN (empty)` is false.
 */
function compileQuantified(
  expression: BoundSubqueryExpression,
  scope: Scope,
  run: (row: Tuple) => Tuple[],
): CompiledExpression {
  const child = required(
    expression.child,
    `the left side of ${expression.subqueryType}`,
  );
  const comparison = required(
    expression.comparisonType,
    `the comparison of ${expression.subqueryType}`,
  );
  const input = scope.compile(child);
  const test = compareTyped(
    comparison,
    child.returnType,
    expression.subplan.types[0] ?? "ANY",
  );
  const kind = expression.subqueryType === "ALL" ? "and" : "or";
  function* comparisons(row: Tuple, value: Value): Generator<Truth> {
    for (const [item = null] of run(row)) yield test(value, item);
  }
  return (row) => foldTruth(kind, comparisons(row, input(row)));
}
