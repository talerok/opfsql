import type { BoundCaseExpression } from "../../sql/binder/types.js";
import { truthOf } from "../../values/index.js";
import { compareTyped } from "./logic.js";
import type { CompiledExpression, Scope } from "./scope.js";

/**
 * CASE: the first WHEN that holds picks its THEN, else ELSE, else NULL.
 * With an operand (`CASE x WHEN v …`) the operand is evaluated once and a
 * WHEN holds when its value equals it — so a NULL operand matches nothing.
 */
export function compileCase(
  expression: BoundCaseExpression,
  scope: Scope,
): CompiledExpression {
  const checks = expression.caseChecks.map((check) => ({
    when: scope.compile(check.when),
    result: scope.compile(check.result),
  }));
  const otherwise =
    expression.elseExpr === null ? null : scope.compile(expression.elseExpr);
  if (expression.operand === undefined) {
    return (row) => {
      for (const check of checks) {
        if (truthOf(check.when(row)) === true) return check.result(row);
      }
      return otherwise === null ? null : otherwise(row);
    };
  }
  const operand = scope.compile(expression.operand);
  // `CASE x WHEN v` compares as `x = v`, affinity included.
  const { returnType } = expression.operand;
  const equals = expression.caseChecks.map((check) =>
    compareTyped("EQUAL", returnType, check.when.returnType),
  );
  return (row) => {
    const value = operand(row);
    for (const [index, check] of checks.entries()) {
      if (equals[index]?.(value, check.when(row)) === true) {
        return check.result(row);
      }
    }
    return otherwise === null ? null : otherwise(row);
  };
}
