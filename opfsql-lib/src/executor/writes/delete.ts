import { type Delete, physical, ROWID_COLUMN } from "../../sql/plan/index.js";
import type { WritableTables } from "../../table/index.js";
import { drain } from "../operator.js";
import { compilePlan } from "../compile.js";
import type { Context } from "../context.js";
import type { WriteOutcome } from "./outcome.js";
import { positionOf, rowidAt } from "./sources.js";

/**
 * Reads the rowid of every row to delete — found by its binding, wherever
 * the source puts it — then deletes each row through the table layer.
 */
export function runDelete(
  statement: Delete<physical.Plan>,
  tables: WritableTables,
  context: Context,
): WriteOutcome {
  const { table, tableIndex, source } = statement;
  const layout = physical.layoutOf(source);
  const rowidPosition = positionOf(layout, tableIndex, ROWID_COLUMN);
  const rowids = drain(compilePlan(source, context)).map((row) =>
    rowidAt(row, rowidPosition),
  );
  const editor = tables.editor(table);
  let changes = 0;
  for (const rowid of rowids) {
    if (editor.delete(rowid)) changes++;
  }
  return { changes, lastInsertRowid: null };
}
