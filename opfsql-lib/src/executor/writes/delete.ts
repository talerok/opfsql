import type { Delete, physical } from "../../sql/plan/index.js";
import type { WritableTables } from "../../table/index.js";
import type { Context } from "../context.js";
import type { WriteOutcome } from "./outcome.js";
import { sourceRowids } from "./sources.js";

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
  const rowids = sourceRowids(source, tableIndex, context);
  const editor = tables.editor(table);
  for (const rowid of rowids) editor.delete(rowid);
  return { changes: rowids.length, lastInsertRowid: null };
}
