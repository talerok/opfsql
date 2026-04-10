import type {
  BoundExpression,
  BoundSubqueryExpression,
} from '../../binder/types.js';
import type { Value, Tuple } from '../types.js';
import type { Resolver } from '../resolve.js';
import type { EvalContext } from './context.js';
import { ExecutorError } from '../errors.js';
import { evaluateExpression } from './index.js';
import { applyComparison } from './helpers.js';

export async function evalSubquery(
  expr: BoundExpression,
  tuple: Tuple,
  resolver: Resolver,
  ctx: EvalContext,
): Promise<Value> {
  const sq = expr as BoundSubqueryExpression;

  // EXISTS/NOT_EXISTS only need to know if ≥1 row exists — limit to 1 for early exit
  if (sq.subqueryType === 'EXISTS' || sq.subqueryType === 'NOT_EXISTS') {
    const rows = await ctx.executeSubplan(sq.subplan, tuple, resolver, 1);
    return sq.subqueryType === 'EXISTS' ? rows.length > 0 : rows.length === 0;
  }

  const rows = await ctx.executeSubplan(sq.subplan, tuple, resolver);

  switch (sq.subqueryType) {
    case 'SCALAR':
      return evalScalar(rows);
    case 'ANY':
      return evalQuantified(sq, rows, tuple, resolver, ctx, false);
    case 'ALL':
      return evalQuantified(sq, rows, tuple, resolver, ctx, true);
  }
}

function evalScalar(rows: Tuple[]): Value {
  if (rows.length === 0) return null;
  if (rows.length > 1) {
    throw new ExecutorError('Scalar subquery returned more than one row');
  }
  return rows[0][0] ?? null;
}

async function evalQuantified(
  sq: BoundSubqueryExpression,
  rows: Tuple[],
  tuple: Tuple,
  resolver: Resolver,
  ctx: EvalContext,
  isAll: boolean,
): Promise<Value> {
  if (!sq.child || !sq.comparisonType) return null;

  const input = await evaluateExpression(sq.child, tuple, resolver, ctx);
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
  // ALL: all non-null passed but had nulls → NULL; no nulls → true
  // ANY: no non-null matched but had nulls → NULL; no nulls → false
  return hasNull ? null : isAll;
}
