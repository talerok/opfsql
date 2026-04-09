import type {
  BoundExpression,
  BoundComparisonExpression,
} from '../../binder/types.js';
import { BoundExpressionClass } from '../../binder/types.js';

export function isComparison(expr: BoundExpression): expr is BoundComparisonExpression {
  return expr.expressionClass === BoundExpressionClass.BOUND_COMPARISON;
}
