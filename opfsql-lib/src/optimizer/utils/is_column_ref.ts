import type {
  BoundExpression,
  BoundColumnRefExpression,
} from '../../binder/types.js';
import { BoundExpressionClass } from '../../binder/types.js';

export function isColumnRef(expr: BoundExpression): expr is BoundColumnRefExpression {
  return expr.expressionClass === BoundExpressionClass.BOUND_COLUMN_REF;
}
