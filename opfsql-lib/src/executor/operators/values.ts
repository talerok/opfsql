import {
  type CompiledExpression,
  compileExpression,
  type Tuple,
} from "../../expressions/index.js";
import type { physical } from "../../sql/plan/index.js";
import { type Context, fixedScope } from "../context.js";
import { leafOperator, type Operator } from "../operator.js";

/**
 * Literal rows, each evaluated when it is pulled: a row past a LIMIT never
 * is. They may read outer rows, so they are evaluated again on each start.
 */
export function values(plan: physical.Values, context: Context): Operator {
  const scope = fixedScope(context);
  const rows = plan.rows.map((row) =>
    row.map((expression) => compileExpression(expression, scope)),
  );
  return leafOperator(() => evaluated(rows), context.query.run);
}

function* evaluated(
  rows: readonly (readonly CompiledExpression[])[],
): Generator<Tuple> {
  for (const row of rows) yield row.map((value) => value([]));
}
