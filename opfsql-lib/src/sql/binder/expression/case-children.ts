import type { BoundCaseExpression, BoundExpression } from "../types.js";

/** An optional child as a list: empty when it is absent. */
export function childList(child: BoundExpression | null): BoundExpression[] {
  return child === null ? [] : [child];
}

/** Every subexpression of a CASE, its operand first if it has one. */
export function caseChildren(
  expression: BoundCaseExpression,
): BoundExpression[] {
  return [
    ...childList(expression.operand),
    ...expression.caseChecks.flatMap((check) => [check.when, check.result]),
    ...childList(expression.elseExpr),
  ];
}
