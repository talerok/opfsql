import type { Case, CaseBranch, physical } from "../sql/plan/index.js";
import { truthOf, type Value } from "../values/index.js";
import { comparison } from "./comparison.js";
import type { CompiledExpression, Compiler, Tuple } from "./scope.js";

/** A WHEN … THEN …: whether it holds for a row, and what it gives then. */
interface CompiledBranch {
  readonly holds: (row: Tuple, operand: Value) => boolean;
  readonly result: CompiledExpression;
}

const alwaysNull: CompiledExpression = () => null;

/** `CASE x WHEN v` compares as `x = v`. */
const equals = comparison("EQUAL");

/**
 * CASE: the first WHEN that holds picks its THEN, else ELSE, else NULL.
 * A simple CASE (`CASE x WHEN v …`) evaluates its operand once per row;
 * a WHEN holds when its value equals the operand, so a NULL operand
 * matches nothing.
 */
export function compileCase(
  expression: Case<physical.Plan>,
  compiler: Compiler,
): CompiledExpression {
  const { operand, otherwise } = expression;
  const operandValue =
    operand === null ? alwaysNull : compiler.compile(operand);
  const branches = expression.branches.map((branch) =>
    compileBranch(branch, operand, compiler),
  );
  const otherwiseValue =
    otherwise === null ? alwaysNull : compiler.compile(otherwise);
  return (row) => {
    const value = operandValue(row);
    const chosen = branches.find((branch) => branch.holds(row, value));
    return (chosen?.result ?? otherwiseValue)(row);
  };
}

function compileBranch(
  branch: CaseBranch<physical.Plan>,
  operand: physical.Expression | null,
  compiler: Compiler,
): CompiledBranch {
  const when = compiler.compile(branch.when);
  const result = compiler.compile(branch.result);
  if (operand === null) {
    return { holds: (row) => truthOf(when(row)) === true, result };
  }
  return { holds: (row, value) => equals(value, when(row)) === true, result };
}
