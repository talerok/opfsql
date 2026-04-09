import type { ConjunctionExpression } from '../../parser/types.js';
import { ExpressionType } from '../../parser/types.js';
import type { BoundConjunctionExpression } from '../types.js';
import { BoundExpressionClass } from '../types.js';
import type { BindContext, AggregateContext } from '../core/context.js';
import type { BindScope } from '../core/scope.js';
import { bindExpression } from './index.js';

export function bindConjunction(
  ctx: BindContext,
  expr: ConjunctionExpression,
  scope: BindScope,
  aggCtx?: AggregateContext,
): BoundConjunctionExpression {
  const children = expr.children.map((c) => bindExpression(ctx, c, scope, aggCtx));
  return {
    expressionClass: BoundExpressionClass.BOUND_CONJUNCTION,
    conjunctionType: expr.type === ExpressionType.CONJUNCTION_AND ? 'AND' : 'OR',
    children,
    returnType: 'BOOLEAN',
  };
}
