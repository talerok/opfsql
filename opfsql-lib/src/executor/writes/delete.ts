import { type Delete, physical, ROWID_COLUMN } from "../../sql/plan/index.js";
import type { Value } from "../../values/index.js";
import { compilePlan, statementContext } from "../compile.js";
import { drain } from "../operator.js";
import type { Outcome, WriteTarget } from "../target.js";
import { editorOf, positionOf, rowidAt } from "./editor.js";

/** Reads every rowid to delete, then deletes each row through the table layer. */
export function runDelete(
  statement: Delete<physical.Plan>,
  target: WriteTarget,
  parameters: readonly Value[],
): Outcome {
  const { table, tableIndex, source } = statement;
  const context = statementContext(target.pages, parameters);
  const layout = physical.layoutOf(source);
  const rowidPosition = positionOf(layout, tableIndex, ROWID_COLUMN);
  const rowids = drain(compilePlan(source, context)).map((row) =>
    rowidAt(row, rowidPosition),
  );
  const editor = editorOf(table, target);
  let changes = 0;
  for (const rowid of rowids) {
    if (editor.delete(rowid)) changes++;
  }
  return { rows: [], changes, lastInsertRowid: null };
}
