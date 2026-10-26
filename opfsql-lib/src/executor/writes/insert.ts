import { InternalError } from "../../errors.js";
import {
  compilePredicate,
  type Layout,
  type Scope,
  type Tuple,
} from "../../expressions/index.js";
import type { Index, Schema, Table } from "../../schema/index.js";
import {
  type ConflictAction,
  type Insert,
  type physical,
  positions,
} from "../../sql/plan/index.js";
import type {
  Conflict,
  ConflictConstraint,
  TableEditor,
  WritableTables,
} from "../../table/index.js";
import type { Value } from "../../values/index.js";
import { compilePlan } from "../compile.js";
import { type Context, scopeOf } from "../context.js";
import { drain } from "../operator.js";
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
  // All of the source first: it may read the table it inserts into.
  const rows = drain(compilePlan(statement.source, context));
  const newRow = compileNewRow(statement.table, statement.columns);
  const insertRow = compileRowInsert(statement, tables, context);
  return tally(rows.map((row) => insertRow(newRow(row))));
}

/** Rows inserted or updated count as changes; the last inserted gives its rowid. */
function tally(outcomes: readonly RowOutcome[]): WriteOutcome {
  let changes = 0;
  let lastInsertRowid: number | null = null;
  for (const outcome of outcomes) {
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

function compileRowInsert(
  statement: Insert<physical.Plan>,
  tables: WritableTables,
  context: Context,
): RowInsert {
  const editor = tables.editor(statement.table);
  const { onConflict } = statement;
  if (onConflict === null) return (values) => inserted(editor.insert(values));
  const constraint = constraintOf(onConflict.target, tables.schema);
  const resolve = compileResolution(onConflict.action, editor, context);
  return (values) => {
    const conflict = editor.findConflict(values, constraint);
    if (conflict === null) return inserted(editor.insert(values));
    if (!isHandled(conflict, constraint)) throw conflict.error;
    return resolve(conflict);
  };
}

/**
 * The schema's own object for the constraint ON CONFLICT names, as the
 * editor's conflicts name it: the plan's may be a copy.
 */
function constraintOf(
  constraint: Index | "rowid" | null,
  schema: Schema,
): ConflictConstraint | null {
  if (constraint === null || constraint === "rowid") return constraint;
  const index = schema.index(constraint.name);
  if (index === null) {
    throw new InternalError(
      `ON CONFLICT names a missing index ${constraint.name}`,
    );
  }
  return index;
}

/** True if ON CONFLICT for `named` (any, if null) handles `conflict`. */
function isHandled(
  conflict: Conflict,
  named: ConflictConstraint | null,
): boolean {
  return named === null || conflict.constraint === named;
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
