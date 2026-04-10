import type { FunctionExpression } from '../../parser/types.js';
import type { BoundExpression, BoundFunctionExpression } from '../types.js';
import { BoundExpressionClass } from '../types.js';
import type { BindContext, AggregateContext } from '../core/context.js';
import type { BindScope } from '../core/scope.js';
import { resolveScalarFunctionReturnType } from '../core/type-check.js';
import { AGGREGATE_FUNCTIONS, bindAggregate } from './aggregate.js';
import { sameAggregate } from './same-expression.js';
import { bindExpression } from './index.js';

export function bindFunction(
  ctx: BindContext,
  expr: FunctionExpression,
  scope: BindScope,
  aggCtx?: AggregateContext,
): BoundExpression {
  const name = expr.function_name.toUpperCase();

  if (AGGREGATE_FUNCTIONS.has(name)) {
    const bound = bindAggregate(ctx, expr, scope);
    if (aggCtx) {
      const match = aggCtx.aggregates.find((a) => sameAggregate(a, bound));
      return match ?? bound;
    }
    return bound;
  }

  const children = expr.children.map((c) => bindExpression(ctx, c, scope, aggCtx));
  const returnType = resolveScalarFunctionReturnType(name, children);
  return {
    expressionClass: BoundExpressionClass.BOUND_FUNCTION,
    functionName: name,
    children,
    returnType,
  } satisfies BoundFunctionExpression;
}
