import type { ParameterExpression } from "../../parser/types.js";
import type { BindContext } from "../core/context.js";
import type { BoundParameterExpression } from "../types.js";
import { BoundExpressionClass } from "../types.js";

/** Parameters have unknown type at bind time — resolved to ANY at runtime. */
export function bindParameter(
  context: BindContext,
  expression: ParameterExpression,
): BoundParameterExpression {
  context.noteParameter(expression.index);
  return {
    expressionClass: BoundExpressionClass.BOUND_PARAMETER,
    index: expression.index,
    returnType: "ANY",
  };
}
