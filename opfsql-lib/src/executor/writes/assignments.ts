import {
  type CompiledExpression,
  compileExpression,
  type Scope,
  type Tuple,
} from "../../expressions/index.js";
import type { Assignment, physical } from "../../sql/plan/index.js";
import type { Value } from "../../values/index.js";

/** SET's assignments compiled: a row's new values from its old ones. */
export type Assign = (old: readonly Value[], source: Tuple) => Value[];

/**
 * Every assignment is computed from the `source` row the statement read,
 * none from another's result: `SET a = b, b = a` swaps.
 */
export function compileAssignments(
  assignments: readonly Assignment<physical.Plan>[],
  scope: Scope,
): Assign {
  const compiled = assignments.map(({ column, value }) => ({
    column,
    value: compileExpression(value, scope),
  }));
  return (old, source) => assigned(old, compiled, source);
}

function assigned(
  old: readonly Value[],
  assignments: readonly { column: number; value: CompiledExpression }[],
  source: Tuple,
): Value[] {
  const values = [...old];
  for (const { column, value } of assignments) values[column] = value(source);
  return values;
}
