import type { LogicalType } from "../../../store/types.js";
import type { CaseExpression, ParsedExpression } from "../../parser/types.js";
import { checkTypeCompatibility, resultType } from "../core/type-check.js";
import type { BoundCaseExpression, BoundExpression } from "../types.js";
import { BoundExpressionClass } from "../types.js";

/**
 * A parsed CASE that may carry its operand (`CASE x WHEN v …`). Until the
 * parser keeps the operand, it rewrites each WHEN to `x = v` and this is
 * absent; with it, the operand is bound once and evaluated once per row.
 */
type CaseWithOperand = CaseExpression & {
  readonly operand?: ParsedExpression | null;
};

/** Binds a CASE; `bind` binds a subexpression in the CASE's scope. */
export function bindCase(
  expression: CaseWithOperand,
  bind: (child: ParsedExpression) => BoundExpression,
): BoundCaseExpression {
  const operand =
    expression.operand === undefined || expression.operand === null
      ? undefined
      : bind(expression.operand);
  const caseChecks = expression.case_checks.map((check) => ({
    when: bind(check.when_expr),
    result: bind(check.then_expr),
  }));
  if (operand !== undefined) {
    // Each WHEN value is compared with the operand as `=` would.
    for (const check of caseChecks) {
      checkTypeCompatibility(operand.returnType, check.when.returnType);
    }
  }
  const elseExpression =
    expression.else_expr === null ? null : bind(expression.else_expr);
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
 * result is typed only if they share a type — INTEGER or REAL is ANY, so
 * arithmetic on it decides by the value, as for COALESCE.
 */
function caseType(types: readonly LogicalType[]): LogicalType {
  let common: LogicalType = "NULL";
  for (const type of types) common = checkTypeCompatibility(common, type);
  return resultType(types);
}
