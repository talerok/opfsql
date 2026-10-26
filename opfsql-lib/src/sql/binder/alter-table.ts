import { SemanticError } from "../../errors.js";
import {
  isReservedName,
  nameKey,
  type Column,
  type Index,
  type Table,
} from "../../schema/index.js";
import {
  AlterType,
  type AlterTableStatement,
  type ColumnDefinition,
} from "../parser/index.js";
import type { AlterAction, AlterTable, logical } from "../plan/index.js";
import { declareColumn } from "./column-definition.js";
import { isStatementError } from "./corruption.js";
import { bindIndexKey, bindKeyText } from "./create-index.js";
import { isSameExpression } from "./same-expression.js";
import { columnNamed, tableNamed } from "./dml.js";
import type { BindContext } from "./scope.js";
import { columnsOf } from "./walk.js";

// ---------------------------------------------------------------------------
// ALTER TABLE ADD COLUMN and DROP COLUMN, as SQLite allows them. A table's
// keys are fixed when it is created: an added column cannot be a PRIMARY KEY
// or UNIQUE, and a NOT NULL one needs a default for the rows already there.
// A dropped column cannot be the rowid, nor in an index or constraint, nor
// the table's last.
// ---------------------------------------------------------------------------

export function bindAlterTable(
  statement: AlterTableStatement,
  context: BindContext,
): AlterTable {
  const table = tableNamed(statement.table, context);
  if (isReservedName(table.name)) {
    throw new SemanticError(`table ${table.name} may not be altered`);
  }
  const action = alterAction(statement, table, context);
  return { kind: "alter-table", table, action };
}

function alterAction(
  statement: AlterTableStatement,
  table: Table,
  context: BindContext,
): AlterAction {
  if (statement.alterType === AlterType.ADD_COLUMN) {
    return addColumn(statement.column, table, context);
  }
  return dropColumn(statement.columnName, table, context);
}

function addColumn(
  definition: ColumnDefinition,
  table: Table,
  context: BindContext,
): AlterAction {
  const declared = declareColumn(definition);
  const { column } = declared;
  const isTaken = table.columns.some(
    (other) => nameKey(other.name) === nameKey(column.name),
  );
  if (isTaken) {
    throw new SemanticError(`duplicate column name: ${column.name}`);
  }
  if (declared.primaryKeys > 0 || declared.isAutoincrement) {
    throw new SemanticError("cannot add a PRIMARY KEY column");
  }
  if (declared.isUnique) {
    throw new SemanticError("cannot add a UNIQUE column");
  }
  if (column.isNotNull && column.defaultValue === null) {
    throw new SemanticError(
      "cannot add a NOT NULL column with default value NULL",
    );
  }
  checkKeysKeepMeaning(table, column, context);
  return { kind: "add-column", column };
}

/**
 * An expression key that would read the new column — `doc.title` reads the
 * JSON column doc until the table has a column title — would change what the
 * index holds for rows already in it, so the column is refused.
 */
function checkKeysKeepMeaning(
  table: Table,
  column: Column,
  context: BindContext,
): void {
  const widened = { ...table, columns: [...table.columns, column] };
  for (const index of context.schema.indexesOf(table.name)) {
    const changed = index.keys.find(
      (key) =>
        key.kind === "expression" && !bindsAlike(key.sql, table, widened),
    );
    if (changed !== undefined) {
      throw new SemanticError(
        `cannot add column ${column.name} to ${table.name}: it would change what index ${index.name} holds`,
      );
    }
  }
}

/** True if the key `sql` reads the same over `widened` as over `table`. */
function bindsAlike(sql: string, table: Table, widened: Table): boolean {
  const before = bindIndexKey(sql, table, 0);
  try {
    return isSameExpression(before, bindKeyText(sql, widened, 0));
  } catch (error) {
    if (isStatementError(error)) return false;
    throw error;
  }
}

function dropColumn(
  name: string,
  table: Table,
  context: BindContext,
): AlterAction {
  const column = columnNamed(table, name);
  const refusal = `cannot drop column ${name} of ${table.name}`;
  if (column === table.rowidColumn) {
    throw new SemanticError(
      `${refusal}: it is the table's INTEGER PRIMARY KEY`,
    );
  }
  if (table.columns.length === 1) {
    throw new SemanticError(`${refusal}: it is the table's only column`);
  }
  const index = context.schema
    .indexesOf(table.name)
    .find((candidate) => usesColumn(candidate, table, column));
  if (index !== undefined) {
    throw new SemanticError(`${refusal}: index ${index.name} uses it`);
  }
  return { kind: "drop-column", column };
}

/** True if a key of `index` is `column`, or an expression that reads it. */
function usesColumn(index: Index, table: Table, column: number): boolean {
  return index.keys.some((key) => {
    if (key.kind === "column") return key.column === column;
    return readsColumn(bindIndexKey(key.sql, table, 0), column);
  });
}

function readsColumn(expression: logical.Expression, column: number): boolean {
  for (const reference of columnsOf(expression)) {
    if (reference.binding.columnIndex === column) return true;
  }
  return false;
}
