import type { BoundExpression } from "../../sql/binder/types.js";
import type { Value } from "../../values/index.js";
import { buildResolver } from "../resolve.js";
import { compileExpression } from "./compile.js";
import { ROW_ONLY_CONTEXT } from "./context.js";

/**
 * The value of an expression without columns, parameters or subqueries,
 * computed by the same compiler that runs queries — so folding a constant
 * at plan time can never disagree with evaluating it per row. Undefined
 * when evaluating fails: the error is left for run time, where it surfaces
 * only if the expression is reached.
 */
export function evaluateConstant(
  expression: BoundExpression,
): Value | undefined {
  try {
    return compileExpression(
      expression,
      buildResolver([]),
      ROW_ONLY_CONTEXT,
    )([]);
  } catch {
    return undefined;
  }
}
