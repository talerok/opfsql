import type { LogicalType } from "../../../store/types.js";
import type { CaseExpression, ParsedExpression } from "../../parser/types.js";
import { checkTypeCompatibility, resultType } from "../core/type-check.js";
import type { BoundCaseExpression, BoundExpression } from "../types.js";
import { BoundExpressionClass } from "../types.js";

/** Binds a CASE; `bind` binds a subexpression in the CASE's scope. */
export function bindCase(
  expression: CaseExpression,
  bind: (child: ParsedExpression) => BoundExpression,
): BoundCaseExpression {
  const operand =
    expression.operand === null ? undefined : bind(expression.operand);
  const caseChecks = expression.caseChecks.map((check) => ({
    when: bind(check.whenExpression),
    result: bind(check.thenExpression),
  }));
  if (operand !== undefined) {
    // Each WHEN value is compared with the operand as `=` would.
    for (const check of caseChecks) {
      checkTypeCompatibility(operand.returnType, check.when.returnType);
    }
  }
  const elseExpression =
    expression.elseExpression === null ? null : bind(expression.elseExpression);
  const results = [
    ...caseChecks.map((check) => check.result),
    ...(elseExpression === null ? [] : [elseExpression]),
  ];
  return {
    expressionClass: BoundExpressionClass.BOUND_CASE,
    ...(operand !== undefined && { operand }),
    caseChecks,
    elseExpr: elseExpression,
    returnType: caseType(results.map((result) => result.returnType)),
  };
}

/**
 * Branches must be of compatible kinds (no TEXT next to INTEGER); the
 * result has their common type, as for COALESCE: INTEGER and REAL make a
 * REAL, other mixes ANY.
 */
function caseType(types: readonly LogicalType[]): LogicalType {
  let common: LogicalType = "NULL";
  for (const type of types) common = checkTypeCompatibility(common, type);
  return resultType(types);
}
