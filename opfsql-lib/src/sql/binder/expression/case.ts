import type { CaseExpression, ParsedExpression } from "../../parser/types.js";
import { ExpressionClass, ExpressionType } from "../../parser/types.js";
import type { LogicalType } from "../../../store/types.js";
import type { AggregateContext, BindContext } from "../core/context.js";
import type { BindScope } from "../core/scope.js";
import { checkTypeCompatibility } from "../core/type-check.js";
import type { BoundCaseExpression } from "../types.js";
import { BoundExpressionClass } from "../types.js";
import { bindExpression } from "./index.js";

export function bindCase(
  ctx: BindContext,
  expr: CaseExpression,
  scope: BindScope,
  aggCtx?: AggregateContext,
): BoundCaseExpression {
  const caseChecks = expr.caseChecks.map((c) => ({
    when: bindExpression(
      ctx,
      whenCondition(expr, c.whenExpression),
      scope,
      aggCtx,
    ),
    then: bindExpression(ctx, c.thenExpression, scope, aggCtx),
  }));
  const elseExpr = expr.elseExpression
    ? bindExpression(ctx, expr.elseExpression, scope, aggCtx)
    : null;

  let returnType: LogicalType =
    caseChecks[0]?.then.returnType ?? elseExpr?.returnType ?? "ANY";
  for (let i = 1; i < caseChecks.length; i++) {
    returnType = checkTypeCompatibility(
      returnType,
      caseChecks[i].then.returnType,
    );
  }
  if (elseExpr) {
    returnType = checkTypeCompatibility(returnType, elseExpr.returnType);
  }

  return {
    expressionClass: BoundExpressionClass.BOUND_CASE,
    caseChecks,
    elseExpr,
    returnType,
  };
}

/** `CASE x WHEN v` tests `x = v`, binding the operand once per WHEN. */
function whenCondition(
  caseExpression: CaseExpression,
  when: ParsedExpression,
): ParsedExpression {
  if (caseExpression.operand === null) return when;
  return {
    expressionClass: ExpressionClass.COMPARISON,
    alias: null,
    type: ExpressionType.COMPARE_EQUAL,
    left: caseExpression.operand,
    right: when,
  };
}
