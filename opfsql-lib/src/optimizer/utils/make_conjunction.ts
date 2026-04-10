import type { BoundExpression } from '../../binder/types.js';
import { BoundExpressionClass } from '../../binder/types.js';

export function makeConjunction(
  exprs: BoundExpression[],
): BoundExpression | null {
  if (exprs.length === 0) return null;
  if (exprs.length === 1) return exprs[0];
  return {
    expressionClass: BoundExpressionClass.BOUND_CONJUNCTION,
    conjunctionType: 'AND',
    children: exprs,
    returnType: 'BOOLEAN',
  };
}
