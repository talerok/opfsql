import { InternalError } from "../../errors.js";
import {
  compilePredicate,
  type Layout,
  type Scope,
  type Tuple,
} from "../../expressions/index.js";
import type { Table } from "../../schema/index.js";
import {
  type ConflictAction,
  type Insert,
  type OnConflict,
  type physical,
  positions,
} from "../../sql/plan/index.js";
import {
  type Conflict,
  type ConflictConstraint,
  isSameConstraint,
  type TableEditor,
  type WritableTables,
} from "../../table/index.js";
import type { Value } from "../../values/index.js";
import { compilePlan } from "../compile.js";
import { type Context, scopeOf } from "../context.js";
import { drain, EVERY_ROW, pull } from "../operator.js";
import { compileAssignments } from "./assignments.js";
import type { WriteOutcome } from "./outcome.js";

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

/** What ON CONFLICT does with the row in the way. */
type Resolution = (conflict: Conflict) => RowOutcome;

type ConflictUpdate = Extract<
  ConflictAction<physical.Plan>,
  { readonly kind: "update" }
>;

const UPDATED: RowOutcome = { kind: "updated" };
const SKIPPED: RowOutcome = { kind: "skipped" };

export function runInsert(
  statement: Insert<physical.Plan>,
  tables: WritableTables,
  context: Context,
): WriteOutcome {
  const { table, onConflict } = statement;
  const source = compilePlan(statement.source, context);
  const newRow = compileNewRow(table, statement.columns);
  const editor = tables.editor(table);
  const insertRow = compileRowInsert(onConflict, editor, context);
  // A source that reads the target table is read whole first, so that it
  // never meets the rows it inserts; any other is written as it comes.
  const rows = statement.readsTarget ? drain(source) : pull(source, EVERY_ROW);
  return insertEach(rows, (row) => insertRow(newRow(row)));
}

/**
 * Inserts each row; rows inserted or updated count as changes, and the last
 * inserted gives its rowid.
 */
function insertEach(
  rows: Iterable<Tuple>,
  insertRow: (row: Tuple) => RowOutcome,
): WriteOutcome {
  let changes = 0;
  let lastInsertRowid: number | null = null;
  for (const row of rows) {
    const outcome = insertRow(row);
    if (outcome.kind === "skipped") continue;
    changes++;
    if (outcome.kind === "inserted") lastInsertRowid = outcome.rowid;
  }
  return { changes, lastInsertRowid };
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

/**
 * Inserts a row, or for ON CONFLICT finds the row in its way first: one in
 * the way under `target` (any, if null) is resolved, any other refuses it.
 */
function compileRowInsert(
  onConflict: OnConflict<physical.Plan> | null,
  editor: TableEditor,
  context: Context,
): RowInsert {
  if (onConflict === null) return (values) => inserted(editor.insert(values));
  const { target } = onConflict;
  const resolve = compileResolution(onConflict.action, editor, context);
  return (values) => {
    const conflict = editor.findConflict(values, target);
    if (conflict === null) return inserted(editor.insert(values));
    if (!isHandled(conflict, target)) throw conflict.error;
    return resolve(conflict);
  };
}

/** True if ON CONFLICT for `named` (any, if null) handles `conflict`. */
function isHandled(
  conflict: Conflict,
  named: ConflictConstraint | null,
): boolean {
  return named === null || isSameConstraint(conflict.constraint, named);
}

function compileResolution(
  action: ConflictAction<physical.Plan>,
  editor: TableEditor,
  context: Context,
): Resolution {
  switch (action.kind) {
    case "nothing": {
      return () => SKIPPED;
    }
    case "update": {
      return compileConflictUpdate(action, editor, context);
    }
  }
}

/**
 * DO UPDATE: the row in the way updated by assignments that read it and the
 * refused row, `excluded`, if the condition holds for them.
 */
function compileConflictUpdate(
  action: ConflictUpdate,
  editor: TableEditor,
  context: Context,
): Resolution {
  const scope = scopeOf(context, conflictLayout(action, editor.table));
  const assign = compileAssignments(action.assignments, scope);
  const holds = compileCondition(action.condition, scope);
  return ({ rowid, refused }) => {
    const existing = rowValues(editor, rowid);
    const pair = [...existing, ...refused];
    if (!holds(pair)) return SKIPPED;
    editor.update(rowid, assign(existing, pair));
    return UPDATED;
  };
}

/** The row in the way, bound to `targetIndex`, then the refused row: each every column. */
function conflictLayout(action: ConflictUpdate, table: Table): Layout {
  const width = table.columns.length;
  return [
    ...positions(action.targetIndex, width),
    ...positions(action.excludedIndex, width),
  ];
}

/** DO UPDATE's WHERE, which holds when there is none. */
function compileCondition(
  condition: physical.Expression | null,
  scope: Scope,
): (row: Tuple) => boolean {
  if (condition === null) return () => true;
  return compilePredicate(condition, scope);
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
