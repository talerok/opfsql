import { compileExpression } from "../../expressions/index.js";
import type { physical } from "../../sql/plan/index.js";
import { type Context, fixedScope } from "../context.js";
import { type Operator, WalkOperator } from "../operator.js";

/** Literal rows, evaluated each time the node starts: they may read outer rows. */
export function values(plan: physical.Values, context: Context): Operator {
  const scope = fixedScope(context);
  const rows = plan.rows.map((row) =>
    row.map((expression) => compileExpression(expression, scope)),
  );
  return new WalkOperator(
    () => rows.map((row) => row.map((value) => value([]))).values(),
    [],
  );
}
