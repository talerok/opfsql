import type { BoundCaseExpression, BoundExpression } from "../types.js";

/** Every subexpression of a CASE, its operand first if it has one. */
export function caseChildren(
  expression: BoundCaseExpression,
): BoundExpression[] {
  return [
    ...(expression.operand === undefined ? [] : [expression.operand]),
    ...expression.caseChecks.flatMap((check) => [check.when, check.result]),
    ...(expression.elseExpr === null ? [] : [expression.elseExpr]),
  ];
}
