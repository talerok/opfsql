import { MisuseError } from "../../errors.js";
import type { Value } from "../../values/index.js";
import type { ExecutionContext } from "./context.js";

/** Parameter `$index + 1`; the statement's parameter count was checked. */
export function parameterValue(
  context: ExecutionContext,
  index: number,
): Value {
  const value = context.parameters[index];
  if (value === undefined) {
    throw new MisuseError(
      `parameter $${String(index + 1)} was not given (${String(context.parameters.length)} given)`,
    );
  }
  return value;
}
