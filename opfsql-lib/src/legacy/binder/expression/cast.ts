import type { CastExpression } from "../../../sql/parser/types.js";
import type { BoundCastExpression } from "../types.js";
import { BoundExpressionClass } from "../types.js";
import type { BindContext, AggregateContext } from "../core/context.js";
import type { BindScope } from "../core/scope.js";
import { mapTypeName } from "../core/type-map.js";
import { bindExpression } from "./index.js";

export function bindCast(
  ctx: BindContext,
  expr: CastExpression,
  scope: BindScope,
  aggCtx?: AggregateContext,
): BoundCastExpression {
  const child = bindExpression(ctx, expr.child, scope, aggCtx);
  const castType = mapTypeName(expr.castType);
  return {
    expressionClass: BoundExpressionClass.BOUND_CAST,
    child,
    castType,
    returnType: castType,
  };
}
