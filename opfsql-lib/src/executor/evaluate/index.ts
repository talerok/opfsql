import type { BoundExpression, BoundAggregateExpression } from '../../binder/types.js';
import { BoundExpressionClass } from '../../binder/types.js';
import type { Value, Tuple } from '../types.js';
import type { Resolver } from '../resolve.js';
import type { EvalContext } from './context.js';
import { ExecutorError } from '../errors.js';
import { evalColumnRef } from './column-ref.js';
import { evalComparison } from './comparison.js';
import { evalConjunction } from './conjunction.js';
import { evalOperator } from './operator.js';
import { evalBetween } from './between.js';
import { evalFunction } from './function.js';
import { evalSubquery } from './subquery.js';
import { evalCase } from './case.js';
import { evalCast } from './cast.js';

export { type EvalContext } from './context.js';

/**
 * Evaluate a BoundExpression against a tuple.
 * Async because subqueries require executing sub-plans.
 */
export async function evaluateExpression(
  expr: BoundExpression,
  tuple: Tuple,
  resolver: Resolver,
  ctx: EvalContext,
): Promise<Value> {
  switch (expr.expressionClass) {
    case BoundExpressionClass.BOUND_COLUMN_REF:
      return evalColumnRef(expr, tuple, resolver, ctx);
    case BoundExpressionClass.BOUND_CONSTANT:
      return expr.value;
    case BoundExpressionClass.BOUND_COMPARISON:
      return evalComparison(expr, tuple, resolver, ctx);
    case BoundExpressionClass.BOUND_CONJUNCTION:
      return evalConjunction(expr, tuple, resolver, ctx);
    case BoundExpressionClass.BOUND_OPERATOR:
      return evalOperator(expr, tuple, resolver, ctx);
    case BoundExpressionClass.BOUND_BETWEEN:
      return evalBetween(expr, tuple, resolver, ctx);
    case BoundExpressionClass.BOUND_FUNCTION:
      return evalFunction(expr, tuple, resolver, ctx);
    case BoundExpressionClass.BOUND_AGGREGATE: {
      // Aggregates are pre-computed by PhysicalHashAggregate,
      // referenced via binding set by the binder
      const agg = expr as BoundAggregateExpression;
      const pos = resolver(agg.binding!);
      if (pos === undefined) {
        throw new ExecutorError(
          `Aggregate binding {tableIndex:${agg.binding!.tableIndex}, columnIndex:${agg.binding!.columnIndex}} not found in layout`,
        );
      }
      return tuple[pos] ?? null;
    }
    case BoundExpressionClass.BOUND_SUBQUERY:
      return evalSubquery(expr, tuple, resolver, ctx);
    case BoundExpressionClass.BOUND_CASE:
      return evalCase(expr, tuple, resolver, ctx);
    case BoundExpressionClass.BOUND_CAST:
      return evalCast(expr, tuple, resolver, ctx);
  }
}
