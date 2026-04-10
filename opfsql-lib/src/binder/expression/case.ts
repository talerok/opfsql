import type { LogicalType } from '../../store/types.js';
import type { CaseExpression } from '../../parser/types.js';
import type { BoundCaseExpression } from '../types.js';
import { BoundExpressionClass } from '../types.js';
import type { BindContext, AggregateContext } from '../core/context.js';
import type { BindScope } from '../core/scope.js';
import { checkTypeCompatibility } from '../core/type-check.js';
import { bindExpression } from './index.js';

export function bindCase(
  ctx: BindContext,
  expr: CaseExpression,
  scope: BindScope,
  aggCtx?: AggregateContext,
): BoundCaseExpression {
  const caseChecks = expr.case_checks.map((c) => ({
    when: bindExpression(ctx, c.when_expr, scope, aggCtx),
    then: bindExpression(ctx, c.then_expr, scope, aggCtx),
  }));
  const elseExpr = expr.else_expr
    ? bindExpression(ctx, expr.else_expr, scope, aggCtx)
    : null;

  let returnType: LogicalType =
    caseChecks[0]?.then.returnType ?? elseExpr?.returnType ?? 'ANY';
  for (let i = 1; i < caseChecks.length; i++) {
    returnType = checkTypeCompatibility(returnType, caseChecks[i].then.returnType);
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
