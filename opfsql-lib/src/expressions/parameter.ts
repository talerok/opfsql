import { MisuseError } from "../errors.js";
import type { physical } from "../sql/plan/index.js";
import type { Value } from "../values/index.js";
import type { Run } from "./scope.js";

/**
 * Parameter `$index + 1` of the run. The engine checks a statement's
 * parameter count before running it; a run built without that check still
 * gets MISUSE here, never a NULL.
 */
export function parameterValue(run: Run, index: number): Value {
  const value = run.parameters[index];
  if (value === undefined) {
    throw new MisuseError(
      `parameter $${String(index + 1)} was not given (${String(run.parameters.length)} given)`,
    );
  }
  return value;
}

/**
 * The value of a constant or a parameter — the same for every row of a
 * run — or undefined for any other expression.
 */
export function fixedValue(
  expression: physical.Expression,
  run: Run,
): Value | undefined {
  if (expression.kind === "constant") return expression.value;
  if (expression.kind === "parameter") {
    return parameterValue(run, expression.index);
  }
  return undefined;
}
