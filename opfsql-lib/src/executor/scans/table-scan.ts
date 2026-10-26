import type { physical } from "../../sql/plan/index.js";
import type { Context } from "../context.js";
import { type Operator, WalkOperator } from "../operator.js";
import { tableAccess } from "./table-access.js";

/** Every row of the table, in rowid order or from the largest rowid down. */
export function tableScan(
  plan: physical.TableScan,
  context: Context,
): Operator {
  const { rows, matching } = tableAccess(plan, context);
  const isForward = plan.direction === "ascending";
  return new WalkOperator(
    () => matching(isForward ? rows.scan() : rows.scanBackward()),
    [],
  );
}
