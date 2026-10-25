import type { ConstantExpression } from "../../parser/types.js";
import type { BoundConstantExpression } from "../types.js";
import { BoundExpressionClass } from "../types.js";

export function bindConstant(
  expr: ConstantExpression,
): BoundConstantExpression {
  return {
    expressionClass: BoundExpressionClass.BOUND_CONSTANT,
    value: expr.value,
    returnType: expr.type,
  };
}
