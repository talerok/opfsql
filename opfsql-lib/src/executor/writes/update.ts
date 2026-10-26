import { physical, ROWID_COLUMN, type Update } from "../../sql/plan/index.js";
import type { Value } from "../../values/index.js";
import { compilePlan, statementContext } from "../compile.js";
import { scopeOf } from "../context.js";
import { drain } from "../operator.js";
import type { Outcome, WriteTarget } from "../target.js";
import { compileAssignments } from "./assignments.js";
import { editorOf, positionOf, rowidAt } from "./editor.js";

/** A row's change: which row, and its new values. */
interface RowChange {
  readonly rowid: number;
  readonly values: readonly Value[];
}

/**
 * Reads every target row, then computes every row's new values from its
 * old ones, then writes them through the table layer: no change can move
 * a row into or out of the scan still finding rows, and no assignment sees
 * another row's change.
 */
export function runUpdate(
  statement: Update<physical.Plan>,
  target: WriteTarget,
  parameters: readonly Value[],
): Outcome {
  const { table, tableIndex, source } = statement;
  const context = statementContext(target.pages, parameters);
  const layout = physical.layoutOf(source);
  const columns = table.columns.map((_, column) =>
    positionOf(layout, tableIndex, column),
  );
  const rowidPosition = positionOf(layout, tableIndex, ROWID_COLUMN);
  const assign = compileAssignments(
    statement.assignments,
    scopeOf(context, layout),
  );
  const rows = drain(compilePlan(source, context));
  const changes: RowChange[] = rows.map((row) => ({
    rowid: rowidAt(row, rowidPosition),
    values: assign(
      columns.map((position) => row[position] ?? null),
      row,
    ),
  }));
  const editor = editorOf(table, target);
  for (const { rowid, values } of changes) editor.update(rowid, values);
  return { rows: [], changes: changes.length, lastInsertRowid: null };
}
