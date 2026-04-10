import type {
  BoundExpression,
  BoundOperatorExpression,
} from '../../binder/types.js';
import { BoundExpressionClass } from '../../binder/types.js';

export function isOperator(expr: BoundExpression): expr is BoundOperatorExpression {
  return expr.expressionClass === BoundExpressionClass.BOUND_OPERATOR;
}
