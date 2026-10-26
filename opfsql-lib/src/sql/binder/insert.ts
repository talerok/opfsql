import { SemanticError } from "../../errors.js";
import type { Index, Table } from "../../schema/index.js";
import type {
  InsertStatement,
  OnConflictAction,
  OnConflictClause,
} from "../parser/index.js";
import type {
  ConflictAction,
  Insert,
  logical,
  OnConflict,
} from "../plan/index.js";
import { bindCondition, refuse } from "./clause.js";
import {
  bindAssignments,
  columnNamed,
  insertSource,
  tableNamed,
} from "./dml.js";
import { clauseOf } from "./expression.js";
import { relation, Scope, type BindContext } from "./scope.js";

// ---------------------------------------------------------------------------
// INSERT: the columns its source fills (all, unless listed; each once), the
// source, and ON CONFLICT. Columns it leaves out get their defaults when the
// executor writes the row. ON CONFLICT names a unique constraint by its
// columns; DO UPDATE reads the row in the way as the table's own name and
// the refused row as `excluded`.
// ---------------------------------------------------------------------------

type Plan = logical.Plan;

export function bindInsert(
  statement: InsertStatement,
  context: BindContext,
): Insert<Plan> {
  const table = tableNamed(statement.table, context);
  const columns = targetColumns(statement.columns, table);
  const source = insertSource(statement.source, columns.length, context);
  const onConflict =
    statement.onConflict === null
      ? null
      : bindOnConflict(statement.onConflict, table, context);
  return {
    kind: "insert",
    table,
    columns,
    source: source.plan,
    onConflict,
    parameterCount: context.parameterCount,
  };
}

function targetColumns(names: readonly string[], table: Table): number[] {
  if (names.length === 0) return table.columns.map((_, position) => position);
  const columns = names.map((name) => columnNamed(table, name));
  const repeated = names.find(
    (_, position) => columns.indexOf(columns[position] ?? -1) !== position,
  );
  if (repeated !== undefined) {
    throw new SemanticError(`column ${repeated} is listed twice in INSERT`);
  }
  return columns;
}

function bindOnConflict(
  clause: OnConflictClause,
  table: Table,
  context: BindContext,
): OnConflict<Plan> {
  const target =
    clause.target === null
      ? null
      : conflictTarget(clause.target, table, context);
  return { target, action: conflictAction(clause.action, table, context) };
}

/** The unique constraint, unique index or rowid whose columns are `names`. */
function conflictTarget(
  names: readonly string[],
  table: Table,
  context: BindContext,
): Index | "rowid" {
  const columns = new Set(names.map((name) => columnNamed(table, name)));
  const { rowidColumn } = table;
  const isRowid =
    columns.size === 1 && rowidColumn !== null && columns.has(rowidColumn);
  if (isRowid) return "rowid";
  const index = context.schema
    .indexesOf(table.name)
    .find((candidate) => isOnColumns(candidate, columns, names.length));
  if (index === undefined) {
    throw new SemanticError(
      "ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint",
    );
  }
  return index;
}

/** A unique index over exactly `columns`, each once, in any order. */
function isOnColumns(
  index: Index,
  columns: ReadonlySet<number>,
  count: number,
): boolean {
  const keyColumns = index.keys
    .filter((key) => key.kind === "column")
    .map((key) => key.column);
  const isSameSet =
    keyColumns.length === index.keys.length &&
    keyColumns.length === count &&
    columns.size === count &&
    keyColumns.every((column) => columns.has(column));
  return index.isUnique && isSameSet;
}

function conflictAction(
  action: OnConflictAction,
  table: Table,
  context: BindContext,
): ConflictAction<Plan> {
  if (action.type === "NOTHING") return { kind: "nothing" };
  const targetIndex = context.newIndex();
  const excludedIndex = context.newIndex();
  const scope = Scope.root(context);
  scope.add(relation(table.name, targetIndex, table.columns));
  scope.add({
    ...relation("excluded", excludedIndex, table.columns),
    isQualifiedOnly: true,
  });
  const clause = clauseOf(scope, refuse("ON CONFLICT"));
  const assignments = bindAssignments(action.assignments, table, clause);
  const condition =
    action.where === null
      ? null
      : bindCondition(action.where, clause, "ON CONFLICT … WHERE");
  return { kind: "update", targetIndex, excludedIndex, assignments, condition };
}
