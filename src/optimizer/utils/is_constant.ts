import type {
  BoundExpression,
  BoundConstantExpression,
} from '../../binder/types.js';
import { BoundExpressionClass } from '../../binder/types.js';

export function isConstant(expr: BoundExpression): expr is BoundConstantExpression {
  return expr.expressionClass === BoundExpressionClass.BOUND_CONSTANT;
}
