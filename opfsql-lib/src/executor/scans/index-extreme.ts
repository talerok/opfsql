import type { physical } from "../../sql/plan/index.js";
import type { Context } from "../context.js";
import { type Operator, WalkOperator } from "../operator.js";
import { compileEqual } from "./seeks.js";

/**
 * MIN or MAX of the key part after `equal`: the first non-NULL value from
 * one end of the index, NULL if there is none — the aggregate's one row.
 */
export function indexExtreme(
  plan: physical.IndexExtreme,
  context: Context,
): Operator {
  const index = context.tables.index(plan.index);
  const equal = compileEqual(plan.equal, context);
  const end = plan.extreme === "MIN" ? "first" : "last";
  return new WalkOperator(() => [[index.extreme(equal(), end)]].values(), []);
}
