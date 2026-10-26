import type { Tuple } from "../../expressions/index.js";
import { physical, ROWID_COLUMN } from "../../sql/plan/index.js";
import type { TableRow } from "../../table/index.js";
import { TableRows } from "../../table/index.js";
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
  /** The found rows' tuples, in the order found, that pass the residual. */
  readonly matching: (found: Iterable<TableRow>) => Generator<Tuple>;
}

export function tableAccess(
  plan: TableAccessPlan,
  context: Context,
): TableAccess {
  const { columns } = plan;
  const residual = compileConditions(
    plan.residual,
    scopeOf(context, physical.layoutOf(plan)),
  );
  function* matching(found: Iterable<TableRow>): Generator<Tuple> {
    for (const row of found) {
      const tuple = tupleOf(row, columns);
      if (residual(tuple)) yield tuple;
    }
  }
  return { rows: new TableRows(context.pages, plan.table), matching };
}

/** The row's values for `columns`, ROWID_COLUMN being its rowid. */
function tupleOf(row: TableRow, columns: readonly number[]): Tuple {
  return columns.map((column) =>
    column === ROWID_COLUMN ? row.rowid : row.value(column),
  );
}
