import type { physical } from "../../sql/plan/index.js";
import type { Context } from "../context.js";
import { type Operator, WalkOperator } from "../operator.js";
import { compileKeySeek } from "./seeks.js";
import { tableAccess } from "./table-access.js";

/** The rows of the index's entries within the key range, in key order or its reverse. */
export function indexScan(
  plan: physical.IndexScan,
  context: Context,
): Operator {
  const { matching } = tableAccess(plan, context);
  const index = context.tables.index(plan.index);
  const seek = compileKeySeek(plan.keys, context);
  return new WalkOperator(
    () => matching(index.rows(seek(), plan.direction)),
    [],
  );
}
