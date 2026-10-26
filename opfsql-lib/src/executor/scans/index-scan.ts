import type { Scope, Tuple } from "../../expressions/index.js";
import type { physical } from "../../sql/plan/index.js";
import { type Context, startScope } from "../context.js";
import { leafOperator, type Operator } from "../operator.js";
import { compileKeySeek, NO_ROW } from "./seeks.js";
import { tableAccess } from "./table-access.js";

/** The rows of the index's entries within the key range, in key order or its reverse. */
export function indexScan(
  plan: physical.IndexScan,
  context: Context,
): Operator {
  const rowsFor = indexScanRows(plan, context, startScope(context));
  return leafOperator(() => rowsFor(NO_ROW), context.query.run);
}

/**
 * The scan's rows for the row it starts for, its seek values compiled in
 * `seeks`: a scan of its own starts for no row; a lookup join's right scan
 * starts for each left row.
 */
export function indexScanRows(
  plan: physical.IndexScan,
  context: Context,
  seeks: Scope,
): (row: Tuple) => Generator<Tuple> {
  const { reader, matching } = tableAccess(plan, context);
  const index = context.tables.index(plan.index);
  const seek = compileKeySeek(plan.keys, seeks);
  return (row) => matching(index.rows(seek(row), plan.direction, reader));
}
