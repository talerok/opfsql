import type {
  BoundExpression,
  BoundConjunctionExpression,
} from '../../binder/types.js';
import { BoundExpressionClass } from '../../binder/types.js';

export function isConjunction(expr: BoundExpression): expr is BoundConjunctionExpression {
  return expr.expressionClass === BoundExpressionClass.BOUND_CONJUNCTION;
}
