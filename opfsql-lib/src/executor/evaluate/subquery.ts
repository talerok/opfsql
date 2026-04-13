import type {
  BoundExpression,
  BoundSubqueryExpression,
} from "../../binder/types.js";
import { applyComparison } from "./utils/compare.js";
import { ExecutorError } from "../errors.js";
import type { Resolver } from "../resolve.js";
import type { Tuple, Value } from "../types.js";
import type { SyncEvalContext } from "./context.js";
import { evaluateExpression } from "./index.js";

export function evalSubquery(
  expr: BoundExpression,
  tuple: Tuple,
  resolver: Resolver,
  ctx: SyncEvalContext,
): Value {
  const sq = expr as BoundSubqueryExpression;

  if (sq.subqueryType === "EXISTS" || sq.subqueryType === "NOT_EXISTS") {
    const rows = ctx.executeSubplan(sq.subplan, tuple, resolver, 1);
    return sq.subqueryType === "EXISTS" ? rows.length > 0 : rows.length === 0;
  }

  const rows = ctx.executeSubplan(sq.subplan, tuple, resolver);

  switch (sq.subqueryType) {
    case "SCALAR":
      return evalScalar(rows);
    case "ANY":
      return evalQuantified(sq, rows, tuple, resolver, ctx, false);
    case "ALL":
      return evalQuantified(sq, rows, tuple, resolver, ctx, true);
  }
}

function evalScalar(rows: Tuple[]): Value {
  if (rows.length === 0) return null;
  if (rows.length > 1) {
    throw new ExecutorError("Scalar subquery returned more than one row");
  }
  return rows[0][0] ?? null;
}

function evalQuantified(
  sq: BoundSubqueryExpression,
  rows: Tuple[],
  tuple: Tuple,
  resolver: Resolver,
  ctx: SyncEvalContext,
  isAll: boolean,
): Value {
  if (!sq.child || !sq.comparisonType) return null;

  const input = evaluateExpression(sq.child, tuple, resolver, ctx);
  if (input === null) return null;
  if (rows.length === 0) return isAll;

  let hasNull = false;
  for (const row of rows) {
    const result = applyComparison(input, row[0], sq.comparisonType);
    if (result === null) {
      hasNull = true;
      continue;
    }
    if (isAll && result !== true) return false;
    if (!isAll && result === true) return true;
  }
  return hasNull ? null : isAll;
}
