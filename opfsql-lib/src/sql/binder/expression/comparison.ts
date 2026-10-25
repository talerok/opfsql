import type { ComparisonExpression } from "../../parser/types.js";
import { ExpressionType } from "../../parser/types.js";
import type { BoundExpression, BoundComparisonExpression } from "../types.js";
import { BoundExpressionClass } from "../types.js";
import type { BindContext, AggregateContext } from "../core/context.js";
import type { BindScope } from "../core/scope.js";
import { checkTypeCompatibility } from "../core/type-check.js";
import { mapComparisonType } from "../core/type-map.js";
import { bindLike } from "./function.js";
import { bindExpression } from "./index.js";

export function bindComparison(
  ctx: BindContext,
  expr: ComparisonExpression,
  scope: BindScope,
  aggCtx?: AggregateContext,
): BoundExpression {
  const left = bindExpression(ctx, expr.left, scope, aggCtx);
  const right = bindExpression(ctx, expr.right, scope, aggCtx);

  if (
    expr.type === ExpressionType.COMPARE_LIKE ||
    expr.type === ExpressionType.COMPARE_NOT_LIKE
  ) {
    const like = bindLike(left, right);
    return expr.type === ExpressionType.COMPARE_LIKE
      ? like
      : {
          expressionClass: BoundExpressionClass.BOUND_OPERATOR,
          operatorType: "NOT",
          children: [like],
          returnType: "BOOLEAN",
        };
  }

  checkTypeCompatibility(left.returnType, right.returnType);
  return {
    expressionClass: BoundExpressionClass.BOUND_COMPARISON,
    comparisonType: mapComparisonType(expr.type),
    left,
    right,
    returnType: "BOOLEAN",
  } satisfies BoundComparisonExpression;
}
