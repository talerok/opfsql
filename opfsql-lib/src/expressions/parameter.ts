import { MisuseError } from "../errors.js";
import type { Parameter, physical } from "../sql/plan/index.js";
import { applyAffinity, type Value } from "../values/index.js";
import type { Run } from "./scope.js";

/**
 * The value of parameter `$index + 1` in the run, converted by its affinity
 * (docs/semantics.md, Comparisons). The engine checks a statement's
 * parameter count before running it; a run built without that check still
 * gets MISUSE here, never a NULL.
 */
export function parameterValue(run: Run, parameter: Parameter): Value {
  const value = run.parameters[parameter.index];
  if (value === undefined) {
    throw new MisuseError(
      `parameter $${String(parameter.index + 1)} was not given (${String(run.parameters.length)} given)`,
    );
  }
  const { affinity } = parameter;
  return affinity === null ? value : applyAffinity(value, affinity);
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
  if (expression.kind === "parameter") return parameterValue(run, expression);
  return undefined;
}
