import { SemanticError } from "../../errors.js";
import {
  isReservedName,
  nameKey,
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
import { bindIndexKey } from "./create-index.js";
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
  const action =
    statement.alterType === AlterType.ADD_COLUMN
      ? addColumn(statement.column, table)
      : dropColumn(statement.columnName, table, context);
  return { kind: "alter-table", table, action };
}

function addColumn(definition: ColumnDefinition, table: Table): AlterAction {
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
  return { kind: "add-column", column };
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
