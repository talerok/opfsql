import { SemanticError } from "../../errors.js";
import type { Index, Table } from "../../schema/index.js";
import type {
  InsertStatement,
  OnConflictAction,
  OnConflictClause,
  ParsedExpression,
} from "../parser/index.js";
import type {
  ConflictAction,
  Insert,
  logical,
  OnConflict,
} from "../plan/index.js";
import { bindCondition, refuse, type Clause } from "./clause.js";
import {
  bindAssignments,
  columnNamed,
  insertSource,
  writableTable,
  writtenTableReads,
} from "./dml.js";
import { firstRepeated } from "./columns.js";
import { clauseOf } from "./expression.js";
import { relation, Scope, type BindContext } from "./scope.js";

// ---------------------------------------------------------------------------
// INSERT: the columns its source fills (all, unless listed; each once), the
// source, and ON CONFLICT. Columns it leaves out get their defaults when the
// executor writes the row. ON CONFLICT names a unique constraint by its
// columns; DO UPDATE reads the existing, conflicting row under the table's
// name and the refused row as `excluded`.
// ---------------------------------------------------------------------------

type Plan = logical.Plan;

export function bindInsert(
  statement: InsertStatement,
  context: BindContext,
): Insert<Plan> {
  const table = writableTable(statement.table, context);
  const columns = targetColumns(statement.columns, table);
  const source = insertSource(statement.source, columns.length, context);
  // Read before ON CONFLICT binds: only the source's reads count.
  const isWrittenTableRead = writtenTableReads(table, context) > 0;
  const onConflict = bindOnConflict(statement.onConflict, table, context);
  return {
    kind: "insert",
    table,
    columns,
    source: source.plan,
    readsTarget: isWrittenTableRead,
    onConflict,
    parameterCount: context.parameterCount,
  };
}

function targetColumns(names: readonly string[], table: Table): number[] {
  if (names.length === 0) return table.columns.map((_, position) => position);
  return namedOnce(names, table, "INSERT");
}

function bindOnConflict(
  clause: OnConflictClause | null,
  table: Table,
  context: BindContext,
): OnConflict<Plan> | null {
  if (clause === null) return null;
  const target = conflictTarget(clause.target, table, context);
  return { target, action: conflictAction(clause.action, table, context) };
}

/**
 * The unique constraint, unique index or rowid whose columns are `names`;
 * null, any constraint, when there are none.
 */
function conflictTarget(
  names: readonly string[] | null,
  table: Table,
  context: BindContext,
): Index | "rowid" | null {
  if (names === null) return null;
  const columns = namedOnce(names, table, "ON CONFLICT");
  const { rowidColumn } = table;
  const isRowid =
    rowidColumn !== null &&
    columns.length === 1 &&
    columns.includes(rowidColumn);
  if (isRowid) return "rowid";
  const index = context.schema
    .indexesOf(table.name)
    .find((candidate) => isOnColumns(candidate, columns));
  if (index === undefined) {
    throw new SemanticError(
      "ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint",
    );
  }
  return index;
}

/** The positions of the columns `names` list, each listed once. */
function namedOnce(
  names: readonly string[],
  table: Table,
  place: string,
): number[] {
  const columns = names.map((name) => columnNamed(table, name));
  const repeated = names[firstRepeated(columns)];
  if (repeated !== undefined) {
    throw new SemanticError(`column ${repeated} is listed twice in ${place}`);
  }
  return columns;
}

/** A unique index over exactly `columns`, in any order. */
function isOnColumns(index: Index, columns: readonly number[]): boolean {
  const keyColumns = index.keys
    .filter((key) => key.kind === "column")
    .map((key) => key.column);
  const isSameSet =
    keyColumns.length === index.keys.length &&
    keyColumns.length === columns.length &&
    keyColumns.every((column) => columns.includes(column));
  return index.isUnique && isSameSet;
}

function bindOptionalCondition(
  where: ParsedExpression | null,
  clause: Clause,
): logical.Expression | null {
  if (where === null) return null;
  return bindCondition(where, clause, "ON CONFLICT … WHERE");
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
  const condition = bindOptionalCondition(action.where, clause);
  return { kind: "update", targetIndex, excludedIndex, assignments, condition };
}
