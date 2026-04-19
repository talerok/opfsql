import type {
  BoundExpression,
  BoundParameterExpression,
} from '../../binder/types.js';
import { BoundExpressionClass } from '../../binder/types.js';

export function isParameter(expr: BoundExpression): expr is BoundParameterExpression {
  return expr.expressionClass === BoundExpressionClass.BOUND_PARAMETER;
}
