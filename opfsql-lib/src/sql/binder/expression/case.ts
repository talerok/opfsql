import type { LogicalType } from "../../../store/types.js";
import type { CaseExpression, ParsedExpression } from "../../parser/types.js";
import { checkTypeCompatibility, resultType } from "../core/type-check.js";
import type { BoundCaseExpression, BoundExpression } from "../types.js";
import { BoundExpressionClass } from "../types.js";

/**
 * Binds a CASE; `bind` binds a subexpression in the CASE's scope. A simple
 * CASE (`CASE x WHEN v …`) keeps its operand, bound once, so it is also
 * evaluated once per row.
 */
export function bindCase(
  expression: CaseExpression,
  bind: (child: ParsedExpression) => BoundExpression,
): BoundCaseExpression {
  const operand = expression.operand === null ? null : bind(expression.operand);
  const caseChecks = expression.caseChecks.map((check) => ({
    when: bind(check.whenExpression),
    result: bind(check.thenExpression),
  }));
  if (operand !== null) {
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
  assertCompatible(results.map((result) => result.returnType));
  return {
    expressionClass: BoundExpressionClass.BOUND_CASE,
    operand,
    caseChecks,
    elseExpr: elseExpression,
    // INTEGER and REAL make a REAL, other mixes ANY: as for COALESCE.
    returnType: resultType(results),
  };
}

/** Branches must be of compatible kinds: no TEXT next to INTEGER. */
function assertCompatible(types: readonly LogicalType[]): void {
  let common: LogicalType = "NULL";
  for (const type of types) common = checkTypeCompatibility(common, type);
}
