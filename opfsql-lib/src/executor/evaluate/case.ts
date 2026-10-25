import type {
  BoundCaseExpression,
  BoundExpression,
} from "../../sql/binder/types.js";
import { truthOf, type Value } from "../../values/index.js";
import type { Tuple } from "../types.js";
import { compareTyped } from "./logic.js";
import type { CompiledExpression, Scope } from "./scope.js";

/** A WHEN … THEN …: whether it holds for a row, and what it gives then. */
interface CompiledCheck {
  readonly holds: (row: Tuple, operand: Value) => boolean;
  readonly result: CompiledExpression;
}

const alwaysNull: CompiledExpression = () => null;

/**
 * CASE: the first WHEN that holds picks its THEN, else ELSE, else NULL.
 * A simple CASE (`CASE x WHEN v …`) evaluates its operand once per row;
 * a WHEN holds when its value equals the operand, so a NULL operand
 * matches nothing.
 */
export function compileCase(
  expression: BoundCaseExpression,
  scope: Scope,
): CompiledExpression {
  const { operand, elseExpr } = expression;
  const operandValue = operand === null ? alwaysNull : scope.compile(operand);
  const checks = expression.caseChecks.map((check) =>
    compileCheck(check, operand, scope),
  );
  const otherwise = elseExpr === null ? alwaysNull : scope.compile(elseExpr);
  return (row) => {
    const value = operandValue(row);
    const chosen = checks.find((check) => check.holds(row, value));
    return (chosen?.result ?? otherwise)(row);
  };
}

function compileCheck(
  check: BoundCaseExpression["caseChecks"][number],
  operand: BoundExpression | null,
  scope: Scope,
): CompiledCheck {
  const when = scope.compile(check.when);
  const result = scope.compile(check.result);
  if (operand === null) {
    return { holds: (row) => truthOf(when(row)) === true, result };
  }
  // `CASE x WHEN v` compares as `x = v`, affinity included.
  const equals = compareTyped(
    "EQUAL",
    operand.returnType,
    check.when.returnType,
  );
  return { holds: (row, value) => equals(value, when(row)) === true, result };
}
