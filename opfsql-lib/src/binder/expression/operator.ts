import type { OperatorExpression } from "../../parser/types.js";
import type { LogicalType } from "../../store/types.js";
import type { AggregateContext, BindContext } from "../core/context.js";
import { BindError } from "../core/errors.js";
import type { BindScope } from "../core/scope.js";
import { checkTypeCompatibility, resolveArithmeticType } from "../core/type-check.js";
import { mapOperatorType } from "../core/type-map.js";
import type { BoundOperatorExpression } from "../types.js";
import { BoundExpressionClass } from "../types.js";
import { bindExpression } from "./index.js";

export function bindOperator(
  ctx: BindContext,
  expr: OperatorExpression,
  scope: BindScope,
  aggCtx?: AggregateContext,
): BoundOperatorExpression {
  const children = expr.children.map((c) =>
    bindExpression(ctx, c, scope, aggCtx),
  );
  const opType = mapOperatorType(expr.type);

  let returnType: LogicalType;
  switch (opType) {
    case "NOT":
    case "IS_NULL":
    case "IS_NOT_NULL":
    case "IN":
    case "NOT_IN":
      for (let i = 1; i < children.length; i++) {
        checkTypeCompatibility(children[0].returnType, children[i].returnType);
      }
      returnType = "BOOLEAN";
      break;
    case "NEGATE":
      returnType = children[0].returnType;
      break;
    case "ADD":
    case "SUBTRACT":
    case "MULTIPLY":
    case "DIVIDE":
    case "MOD":
      returnType = resolveArithmeticType(
        children[0].returnType,
        children[1].returnType,
      );
      break;
    case "CONCAT":
      if (children.some((c) => c.returnType === "BLOB")) {
        throw new BindError(`Cannot apply CONCAT to BLOB type`);
      }
      returnType = "TEXT";
      break;
    default:
      returnType = "ANY";
  }

  return {
    expressionClass: BoundExpressionClass.BOUND_OPERATOR,
    operatorType: opType,
    children,
    returnType,
  };
}
