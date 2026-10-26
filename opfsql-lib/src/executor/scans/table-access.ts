import type { Tuple } from "../../expressions/index.js";
import { physical } from "../../sql/plan/index.js";
import type { ColumnReader, TableRows } from "../../table/index.js";
import { compileConditions, type Context, scopeOf } from "../context.js";

/** A scan node: every one reads one table, outputs columns and checks a residual. */
export type TableAccessPlan = Extract<
  physical.Plan,
  {
    readonly kind: "table-scan" | "rowid-lookup" | "index-scan" | "index-union";
  }
>;

/** What every scan does with the rows its access path finds. */
export interface TableAccess {
  /** The table's rows, read at the transaction's snapshot. */
  readonly rows: TableRows;
  /** The scan's columns of each row, decoded straight from its record. */
  readonly reader: ColumnReader;
  /** The rows found, in the order found, that pass the residual. */
  readonly matching: (found: Generator<Tuple>) => Generator<Tuple>;
}

export function tableAccess(
  plan: TableAccessPlan,
  context: Context,
): TableAccess {
  const rows = context.tables.rows(plan.table);
  const reader = rows.columns(plan.columns);
  if (plan.residual.length === 0) {
    return { rows, reader, matching: (found) => found };
  }
  const passes = compileConditions(
    plan.residual,
    scopeOf(context, physical.layoutOf(plan)),
  );
  function* matching(found: Generator<Tuple>): Generator<Tuple> {
    for (const row of found) {
      if (passes(row)) yield row;
    }
  }
  return { rows, reader, matching };
}
