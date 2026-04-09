import type {
  BoundExpression,
  BoundConjunctionExpression,
} from '../../binder/types.js';
import { BoundExpressionClass } from '../../binder/types.js';

export function flattenConjunction(expr: BoundExpression): BoundExpression[] {
  if (
    expr.expressionClass === BoundExpressionClass.BOUND_CONJUNCTION &&
    (expr as BoundConjunctionExpression).conjunctionType === 'AND'
  ) {
    const conj = expr as BoundConjunctionExpression;
    const result: BoundExpression[] = [];
    for (const child of conj.children) {
      result.push(...flattenConjunction(child));
    }
    return result;
  }
  return [expr];
}
