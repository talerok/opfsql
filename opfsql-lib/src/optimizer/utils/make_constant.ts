import type {
  BoundConstantExpression,
  LogicalType,
} from '../../binder/types.js';
import { BoundExpressionClass } from '../../binder/types.js';

export function makeConstant(
  value: string | number | boolean | null,
  returnType: LogicalType,
): BoundConstantExpression {
  return {
    expressionClass: BoundExpressionClass.BOUND_CONSTANT,
    value,
    returnType,
  };
}
