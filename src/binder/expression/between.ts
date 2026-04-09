import type { BetweenExpression } from '../../parser/types.js';
import type { BoundBetweenExpression } from '../types.js';
import { BoundExpressionClass } from '../types.js';
import type { BindContext, AggregateContext } from '../core/context.js';
import type { BindScope } from '../core/scope.js';
import { checkTypeCompatibility } from '../core/type-check.js';
import { bindExpression } from './index.js';

export function bindBetween(
  ctx: BindContext,
  expr: BetweenExpression,
  scope: BindScope,
  aggCtx?: AggregateContext,
): BoundBetweenExpression {
  const input = bindExpression(ctx, expr.input, scope, aggCtx);
  const lower = bindExpression(ctx, expr.lower, scope, aggCtx);
  const upper = bindExpression(ctx, expr.upper, scope, aggCtx);
  checkTypeCompatibility(input.returnType, lower.returnType);
  checkTypeCompatibility(input.returnType, upper.returnType);
  return {
    expressionClass: BoundExpressionClass.BOUND_BETWEEN,
    input,
    lower,
    upper,
    returnType: 'BOOLEAN',
  };
}
