import { InternalError } from "../../errors.js";
import { compilePredicate, type Tuple } from "../../expressions/index.js";
import type { Table } from "../../schema/index.js";
import {
  type ConflictAction,
  type Insert,
  logical,
  type physical,
} from "../../sql/plan/index.js";
import type { ConflictConstraint, TableEditor } from "../../table/index.js";
import { assignValue, type Value } from "../../values/index.js";
import { compilePlan, statementContext } from "../compile.js";
import { type Context, scopeOf } from "../context.js";
import { drain } from "../operator.js";
import type { Outcome, WriteTarget } from "../target.js";
import { compileAssignments } from "./assignments.js";
import { constraintOf, editorOf } from "./editor.js";

// ---------------------------------------------------------------------------
// INSERT, through the table layer. Without ON CONFLICT a row that breaks a
// constraint fails the statement. With it, a row in the way under the
// constraint it names (or any, if it names none) is left alone or updated;
// one in the way under another constraint still fails.
// ---------------------------------------------------------------------------

/** What inserting one row did. */
type RowOutcome =
  | { readonly kind: "inserted"; readonly rowid: number }
  | { readonly kind: "updated" }
  | { readonly kind: "skipped" };

/** Inserts one row of every column's value. */
type RowInsert = (values: readonly Value[]) => RowOutcome;

/** What ON CONFLICT does with the row in the way, given the refused values. */
type Resolution = (rowid: number, values: readonly Value[]) => RowOutcome;

/** What compiling the statement's row inserts needs. */
interface Inserting {
  readonly table: Table;
  readonly editor: TableEditor;
  readonly context: Context;
}

const UPDATED: RowOutcome = { kind: "updated" };
const SKIPPED: RowOutcome = { kind: "skipped" };

export function runInsert(
  statement: Insert<physical.Plan>,
  target: WriteTarget,
  parameters: readonly Value[],
): Outcome {
  const { table } = statement;
  const context = statementContext(target.pages, parameters);
  // All of the source first: it may read the table it inserts into.
  const rows = drain(compilePlan(statement.source, context));
  const inserting = { table, editor: editorOf(table, target), context };
  const insertRow = compileRowInsert(statement, inserting, target);
  const newRow = compileNewRow(table, statement.columns);
  let changes = 0;
  let lastInsertRowid: number | null = null;
  for (const row of rows) {
    const done = insertRow(newRow(row));
    if (done.kind === "skipped") continue;
    changes++;
    if (done.kind === "inserted") lastInsertRowid = done.rowid;
  }
  return { rows: [], changes, lastInsertRowid };
}

/** A source row as a row of the table: the columns it fills, defaults in the rest. */
function compileNewRow(
  table: Table,
  columns: readonly number[],
): (source: Tuple) => Value[] {
  const defaults = table.columns.map((column) => column.defaultValue);
  return (source) => {
    const values = [...defaults];
    for (const [position, column] of columns.entries()) {
      values[column] = source[position] ?? null;
    }
    return values;
  };
}

function compileRowInsert(
  statement: Insert<physical.Plan>,
  inserting: Inserting,
  target: WriteTarget,
): RowInsert {
  const { editor } = inserting;
  const { onConflict } = statement;
  if (onConflict === null) return (values) => inserted(editor.insert(values));
  const constraint = constraintOf(onConflict.target, target);
  const resolve = compileResolution(onConflict.action, inserting);
  return (values) => {
    const conflict = editor.findConflict(values, constraint);
    if (conflict === null) return inserted(editor.insert(values));
    if (!isHandled(conflict.constraint, constraint)) throw conflict.error;
    return resolve(conflict.rowid, values);
  };
}

/** True if ON CONFLICT for `named` (any, if null) handles a conflict under `found`. */
function isHandled(
  found: ConflictConstraint,
  named: ConflictConstraint | null,
): boolean {
  return named === null || found === named;
}

function compileResolution(
  action: ConflictAction<physical.Plan>,
  inserting: Inserting,
): Resolution {
  switch (action.kind) {
    case "nothing": {
      return () => SKIPPED;
    }
    case "update": {
      return compileConflictUpdate(action, inserting);
    }
  }
}

/**
 * DO UPDATE: its assignments and condition read the row in the way
 * (`targetIndex`) and the refused row (`excluded`), each every column of
 * the table; the refused row's values converted as storing them would.
 */
function compileConflictUpdate(
  action: Extract<ConflictAction<physical.Plan>, { readonly kind: "update" }>,
  { table, editor, context }: Inserting,
): Resolution {
  const width = table.columns.length;
  const layout = [
    ...logical.positions(action.targetIndex, width),
    ...logical.positions(action.excludedIndex, width),
  ];
  const scope = scopeOf(context, layout);
  const assign = compileAssignments(action.assignments, scope);
  const condition = action.condition;
  const holds = condition === null ? null : compilePredicate(condition, scope);
  return (rowid, values) => {
    const existing = rowValues(editor, rowid);
    const pair = [...existing, ...storedValues(table, values)];
    if (holds !== null && !holds(pair)) return SKIPPED;
    editor.update(rowid, assign(existing, pair));
    return UPDATED;
  };
}

function inserted(rowid: number): RowOutcome {
  return { kind: "inserted", rowid };
}

function rowValues(editor: TableEditor, rowid: number): Value[] {
  const row = editor.get(rowid);
  if (row === null) {
    throw new InternalError(`the conflicting row ${String(rowid)} is gone`);
  }
  return row.values();
}

/** `values` as the table would store them; findConflict has checked they fit. */
function storedValues(table: Table, values: readonly Value[]): Value[] {
  return table.columns.map((column, position) =>
    assignValue(values[position] ?? null, column.type),
  );
}
