import type { ConstantExpression } from '../../parser/types.js';
import type { BoundConstantExpression, JsonValue } from '../types.js';
import { BoundExpressionClass } from '../types.js';
import { mapParserType } from '../core/type-map.js';

export function bindConstant(expr: ConstantExpression): BoundConstantExpression {
  if (expr.value.is_null) {
    return {
      expressionClass: BoundExpressionClass.BOUND_CONSTANT,
      value: null,
      returnType: 'NULL',
    };
  }
  return {
    expressionClass: BoundExpressionClass.BOUND_CONSTANT,
    value: expr.value.value as string | number | boolean | JsonValue | null,
    returnType: mapParserType(expr.value.type),
  };
}
