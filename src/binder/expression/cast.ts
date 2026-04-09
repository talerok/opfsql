import type { CastExpression } from '../../parser/types.js';
import type { BoundCastExpression } from '../types.js';
import { BoundExpressionClass } from '../types.js';
import type { BindContext, AggregateContext } from '../core/context.js';
import type { BindScope } from '../core/scope.js';
import { mapParserType } from '../core/type-map.js';
import { bindExpression } from './index.js';

export function bindCast(
  ctx: BindContext,
  expr: CastExpression,
  scope: BindScope,
  aggCtx?: AggregateContext,
): BoundCastExpression {
  const child = bindExpression(ctx, expr.child, scope, aggCtx);
  const castType = mapParserType(expr.cast_type);
  return {
    expressionClass: BoundExpressionClass.BOUND_CAST,
    child,
    castType,
    returnType: castType,
  };
}
