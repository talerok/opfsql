import type { ComparisonExpression } from "../../parser/types.js";
import { ExpressionType } from "../../parser/types.js";
import type { BoundComparisonExpression, BoundExpression } from "../types.js";
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

  // The parser writes NOT LIKE as NOT around LIKE.
  if (expr.type === ExpressionType.COMPARE_LIKE) return bindLike(left, right);

  checkTypeCompatibility(left.returnType, right.returnType);
  return {
    expressionClass: BoundExpressionClass.BOUND_COMPARISON,
    comparisonType: mapComparisonType(expr.type),
    left,
    right,
    returnType: "BOOLEAN",
  } satisfies BoundComparisonExpression;
}
