import type { ParameterExpression } from '../../parser/types.js';
import type { BoundParameterExpression } from '../types.js';
import { BoundExpressionClass } from '../types.js';

/** Parameters have unknown type at bind time — resolved to ANY at runtime. */
export function bindParameter(expr: ParameterExpression): BoundParameterExpression {
  return {
    expressionClass: BoundExpressionClass.BOUND_PARAMETER,
    index: expr.index,
    returnType: 'ANY',
  };
}
