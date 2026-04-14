import type { BoundParameterExpression } from "../../binder/types.js";
import { ExecutorError } from "../errors.js";
import type { Value } from "../types.js";
import type { SyncEvalContext } from "./context.js";

export function evalParameter(expr: BoundParameterExpression, ctx: SyncEvalContext): Value {
  const params = ctx.params;
  if (!params) throw new ExecutorError("No parameters provided for parameterized query");
  if (expr.index >= params.length) {
    throw new ExecutorError(`Parameter $${expr.index + 1} not provided (${params.length} params given)`);
  }
  return params[expr.index];
}
