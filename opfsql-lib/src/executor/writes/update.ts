import type { Layout } from "../../expressions/index.js";
import {
  type physical,
  positions,
  ROWID_COLUMN,
  type Update,
} from "../../sql/plan/index.js";
import type { TableEditor, WritableTables } from "../../table/index.js";
import type { Value } from "../../values/index.js";
import { type Context, scopeOf } from "../context.js";
import { compileAssignments } from "./assignments.js";
import type { WriteOutcome } from "./outcome.js";
import { sourceRowids } from "./sources.js";

// ---------------------------------------------------------------------------
// UPDATE (docs/planning.md, Execution). The rowids of the rows to change are
// read from the source first, so no change moves a row into or out of the
// scan still finding rows. Then each row is read by its rowid through the
// table layer, its new values are computed from it, and they are written.
// A new row depends only on its own old row, which nothing has touched yet,
// so this is the result of computing every new row first (docs/semantics.md,
// Departures from SQLite) — unless an assignment's subquery reads the target
// table: then every new row is computed before any is written. Memory holds
// the rowids, and only in that case the new rows.
// ---------------------------------------------------------------------------

/** A row's new values, every column's, from its old ones. */
type NewRow = (old: readonly Value[], rowid: number) => Value[];

/** A row's change: which row, and its new values. */
interface RowChange {
  readonly rowid: number;
  readonly values: readonly Value[];
}

export function runUpdate(
  statement: Update<physical.Plan>,
  tables: WritableTables,
  context: Context,
): WriteOutcome {
  const { table, tableIndex, source } = statement;
  const rowids = sourceRowids(source, tableIndex, context);
  const newRow = compileNewRow(statement, context);
  const editor = tables.editor(table);
  const write = statement.readsTarget ? computedFirst : oneAtATime;
  write(rowids, newRow, editor);
  return { changes: rowids.length, lastInsertRowid: null };
}

/** Each row read, its new values computed and written, before the next is read. */
function oneAtATime(
  rowids: readonly number[],
  newRow: NewRow,
  editor: TableEditor,
): void {
  for (const rowid of rowids) {
    editor.updateWith(rowid, (old) => newRow(old, rowid));
  }
}

/** Every new row computed from the table as it was, then each written. */
function computedFirst(
  rowids: readonly number[],
  newRow: NewRow,
  editor: TableEditor,
): void {
  const changes: RowChange[] = rowids.map((rowid) => ({
    rowid,
    values: newRow(editor.rowValues(rowid), rowid),
  }));
  for (const { rowid, values } of changes) editor.update(rowid, values);
}

/**
 * The assignments, compiled over the row as the table layer reads it:
 * every column in column order, then the rowid, bound to `tableIndex`.
 */
function compileNewRow(
  { table, tableIndex, assignments }: Update<physical.Plan>,
  context: Context,
): NewRow {
  const layout: Layout = [
    ...positions(tableIndex, table.columns.length),
    { tableIndex, columnIndex: ROWID_COLUMN },
  ];
  const assign = compileAssignments(assignments, scopeOf(context, layout));
  return (old, rowid) => assign(old, [...old, rowid]);
}
