import type { SubqueryExpression } from "../../parser/types.js";
import type { LogicalType } from "../../store/types.js";
import type { BindContext } from "../core/context.js";
import type { BindScope } from "../core/scope.js";
import { mapComparisonType } from "../core/type-map.js";
import { bindQueryNode } from "../statement/query-node.js";
import type { BoundSubqueryExpression } from "../types.js";
import { BoundExpressionClass } from "../types.js";
import { bindExpression } from "./index.js";

export function bindSubquery(
  ctx: BindContext,
  expr: SubqueryExpression,
  scope: BindScope,
): BoundSubqueryExpression {
  const childScope = scope.createChildScope();
  const subplan = bindQueryNode(ctx, expr.subquery.node, childScope);

  let returnType: LogicalType;
  switch (expr.subquery_type) {
    case "SCALAR":
      returnType = subplan.types[0] ?? "ANY";
      break;
    case "EXISTS":
    case "NOT_EXISTS":
    case "ANY":
    case "ALL":
      returnType = "BOOLEAN";
      break;
    default:
      returnType = "ANY";
  }

  const result: BoundSubqueryExpression = {
    expressionClass: BoundExpressionClass.BOUND_SUBQUERY,
    subqueryType: expr.subquery_type,
    subplan,
    returnType,
  };

  if (expr.child) {
    result.child = bindExpression(ctx, expr.child, scope);
  }
  if (expr.comparison_type) {
    result.comparisonType = mapComparisonType(expr.comparison_type);
  }

  return result;
}
