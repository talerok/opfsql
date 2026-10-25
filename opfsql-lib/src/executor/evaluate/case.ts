import type { BoundCaseExpression } from "../../sql/binder/types.js";
import { compareSql, truthOf, type Value } from "../../values/index.js";
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
  const operand =
    expression.operand === undefined ? null : scope.compile(expression.operand);
  const holds =
    operand === null
      ? (when: Value) => truthOf(when) === true
      : (when: Value, value: Value) => compareSql(value, when) === 0;
  return (row) => {
    const value = operand === null ? null : operand(row);
    for (const check of checks) {
      if (holds(check.when(row), value)) return check.result(row);
    }
    return otherwise === null ? null : otherwise(row);
  };
}
