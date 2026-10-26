import type { Tuple } from "../../expressions/index.js";
import { physical, ROWID_COLUMN, type Update } from "../../sql/plan/index.js";
import type { WritableTables } from "../../table/index.js";
import type { Value } from "../../values/index.js";
import { compilePlan } from "../compile.js";
import { type Context, scopeOf } from "../context.js";
import { drain } from "../operator.js";
import { compileAssignments } from "./assignments.js";
import type { WriteOutcome } from "./outcome.js";
import { positionOf, rowidAt } from "./sources.js";

/** A row's change: which row, and its new values. */
interface RowChange {
  readonly rowid: number;
  readonly values: readonly Value[];
}

/**
 * Reads every target row and decides its new values from its old ones,
 * then writes them through the table layer: no change can move a row into
 * or out of the scan still finding rows, and no assignment sees another
 * row's change (docs/semantics.md, Departures from SQLite).
 */
export function runUpdate(
  statement: Update<physical.Plan>,
  tables: WritableTables,
  context: Context,
): WriteOutcome {
  const changeOf = compileRowChange(statement, context);
  const rows = drain(compilePlan(statement.source, context));
  const changes = rows.map((row) => changeOf(row));
  const editor = tables.editor(statement.table);
  for (const { rowid, values } of changes) editor.update(rowid, values);
  return { changes: changes.length, lastInsertRowid: null };
}

/** A source row's change: its rowid, and its values with the assignments made. */
function compileRowChange(
  { table, tableIndex, source, assignments }: Update<physical.Plan>,
  context: Context,
): (row: Tuple) => RowChange {
  const layout = physical.layoutOf(source);
  const columns = table.columns.map((_, column) =>
    positionOf(layout, tableIndex, column),
  );
  const rowidPosition = positionOf(layout, tableIndex, ROWID_COLUMN);
  const assign = compileAssignments(assignments, scopeOf(context, layout));
  return (row) => {
    const old = columns.map((position) => row[position] ?? null);
    return { rowid: rowidAt(row, rowidPosition), values: assign(old, row) };
  };
}
