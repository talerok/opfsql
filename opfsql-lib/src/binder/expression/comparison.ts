import type { ComparisonExpression } from '../../parser/types.js';
import { ExpressionType } from '../../parser/types.js';
import type { BoundExpression, BoundFunctionExpression, BoundComparisonExpression } from '../types.js';
import { BoundExpressionClass } from '../types.js';
import type { BindContext, AggregateContext } from '../core/context.js';
import { BindError } from '../core/errors.js';
import type { BindScope } from '../core/scope.js';
import { checkTypeCompatibility } from '../core/type-check.js';
import { mapComparisonType } from '../core/type-map.js';
import { bindExpression } from './index.js';

export function bindComparison(
  ctx: BindContext,
  expr: ComparisonExpression,
  scope: BindScope,
  aggCtx?: AggregateContext,
): BoundExpression {
  const left = bindExpression(ctx, expr.left, scope, aggCtx);
  const right = bindExpression(ctx, expr.right, scope, aggCtx);

  if (
    expr.type === ExpressionType.COMPARE_LIKE ||
    expr.type === ExpressionType.COMPARE_NOT_LIKE
  ) {
    if (left.returnType === 'BLOB' || right.returnType === 'BLOB') {
      throw new BindError('Cannot apply LIKE to BLOB type');
    }
    if (left.returnType === 'JSON' || right.returnType === 'JSON') {
      throw new BindError('Cannot apply LIKE to JSON type');
    }
    return {
      expressionClass: BoundExpressionClass.BOUND_FUNCTION,
      functionName: expr.type === ExpressionType.COMPARE_LIKE ? 'LIKE' : 'NOT_LIKE',
      children: [left, right],
      returnType: 'BOOLEAN',
    } satisfies BoundFunctionExpression;
  }

  checkTypeCompatibility(left.returnType, right.returnType);
  return {
    expressionClass: BoundExpressionClass.BOUND_COMPARISON,
    comparisonType: mapComparisonType(expr.type),
    left,
    right,
    returnType: 'BOOLEAN',
  } satisfies BoundComparisonExpression;
}
