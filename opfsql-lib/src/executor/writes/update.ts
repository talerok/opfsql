import type { Tuple } from "../../expressions/index.js";
import { physical, ROWID_COLUMN, type Update } from "../../sql/plan/index.js";
import type { Value } from "../../values/index.js";
import { compilePlan, statementContext } from "../compile.js";
import { type Context, scopeOf } from "../context.js";
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
 * Reads every target row and decides its new values from its old ones,
 * then writes them through the table layer: no change can move a row into
 * or out of the scan still finding rows, and no assignment sees another
 * row's change.
 */
export function runUpdate(
  statement: Update<physical.Plan>,
  target: WriteTarget,
  parameters: readonly Value[],
): Outcome {
  const context = statementContext(target.pages, parameters);
  const changeOf = compileRowChange(statement, context);
  const rows = drain(compilePlan(statement.source, context));
  const changes = rows.map((row) => changeOf(row));
  const editor = editorOf(statement.table, target);
  for (const { rowid, values } of changes) editor.update(rowid, values);
  return { rows: [], changes: changes.length, lastInsertRowid: null };
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
