import { SemanticError } from "../../errors.js";
import {
  constraintIndexName,
  isReservedName,
  nameKey,
  type IndexOrigin,
  type IndexShape,
  type Schema,
  type TableShape,
} from "../../schema/index.js";
import type { CreateTableStatement } from "../parser/index.js";
import type { CreateTable } from "../plan/index.js";
import { declareColumn, type DeclaredColumn } from "./column-definition.js";
import { columnIn, firstRepeated } from "./columns.js";

// ---------------------------------------------------------------------------
// CREATE TABLE to the schema model. A sole PRIMARY KEY column of type
// INTEGER is the rowid (docs/semantics.md, Primary keys); any other primary
// key, and each UNIQUE column, is a constraint index, named
// constraintIndexName(table, n) in the order the table's statement writes
// them: each column's constraints in column order, PRIMARY KEY before
// UNIQUE, then a primary key over several columns. A one-column table
// PRIMARY KEY is that column's.
// ---------------------------------------------------------------------------

export function bindCreateTable(
  statement: CreateTableStatement,
  schema: Schema,
): CreateTable {
  checkNewName(statement.table, "table", schema, statement.ifNotExists);
  const { table, constraints } = tableDefinition(statement);
  return {
    kind: "create-table",
    table,
    constraints,
    isIfNotExists: statement.ifNotExists,
  };
}

/**
 * Checks that a user statement may create `name`: not reserved, and free
 * — tables and indexes share one namespace — unless IF NOT EXISTS and it
 * names an object of the same kind.
 */
export function checkNewName(
  name: string,
  kind: "table" | "index",
  schema: Schema,
  isIfNotExists: boolean,
): void {
  if (isReservedName(name)) {
    throw new SemanticError(
      `${name} is reserved: names starting with opfsql_ belong to opfsql`,
    );
  }
  const existing = kindNamed(name, schema);
  if (existing === null) return;
  if (isIfNotExists && existing === kind) return;
  const article = existing === "index" ? "an" : "a";
  throw new SemanticError(
    `there is already ${article} ${existing} named ${name}`,
  );
}

function kindNamed(name: string, schema: Schema): "table" | "index" | null {
  if (schema.table(name) !== null) return "table";
  if (schema.index(name) !== null) return "index";
  return null;
}

/** The table and constraint indexes a CREATE TABLE describes; names are not checked. */
export function tableDefinition(statement: CreateTableStatement): {
  readonly table: TableShape;
  readonly constraints: IndexShape[];
} {
  const declared = statement.columns.map((column) => declareColumn(column));
  checkColumnNames(statement.table, declared);
  const key = primaryKey(statement, declared);
  const rowidColumn = rowidOf(key, declared);
  checkAutoincrement(declared, rowidColumn);
  const table: TableShape = {
    name: statement.table,
    columns: declared.map(({ column }, position) => {
      const isKeyColumn = position !== rowidColumn && key.includes(position);
      return isKeyColumn ? { ...column, isNotNull: true } : column;
    }),
    rowidColumn,
    isAutoincrement: declared.some((column) => column.isAutoincrement),
  };
  return {
    table,
    constraints: constraintIndexes(statement.table, declared, key, rowidColumn),
  };
}

function checkColumnNames(
  table: string,
  declared: readonly DeclaredColumn[],
): void {
  const names = declared.map(({ column }) => column.name);
  const repeated = names[firstRepeated(names.map((name) => nameKey(name)))];
  if (repeated !== undefined) {
    throw new SemanticError(`duplicate column name in ${table}: ${repeated}`);
  }
}

/** The primary key's columns, however it is written; one at most. */
function primaryKey(
  statement: CreateTableStatement,
  declared: readonly DeclaredColumn[],
): number[] {
  const columns = declared.map(({ column }) => column);
  const columnKeys = declared.flatMap((column, position) =>
    Array.from({ length: column.primaryKeys }, () => [position]),
  );
  const tableKeys = statement.primaryKeys.map((names) =>
    names.map((name) => columnIn(columns, name, statement.table)),
  );
  const keys = [...columnKeys, ...tableKeys];
  if (keys.length > 1) {
    throw new SemanticError(
      `table "${statement.table}" has more than one primary key`,
    );
  }
  const [key = []] = keys;
  if (firstRepeated(key) !== -1) {
    throw new SemanticError(
      `a column is named twice in the primary key of ${statement.table}`,
    );
  }
  return key;
}

/** A primary key that is one INTEGER column is the rowid. */
function rowidOf(
  key: readonly number[],
  declared: readonly DeclaredColumn[],
): number | null {
  const [only, other] = key;
  if (only === undefined || other !== undefined) return null;
  return declared[only]?.column.type === "INTEGER" ? only : null;
}

function checkAutoincrement(
  declared: readonly DeclaredColumn[],
  rowidColumn: number | null,
): void {
  const misplaced = declared.find(
    (column, position) => column.isAutoincrement && position !== rowidColumn,
  );
  if (misplaced !== undefined) {
    throw new SemanticError(
      `AUTOINCREMENT on column ${misplaced.column.name}: it is allowed only on an INTEGER PRIMARY KEY`,
    );
  }
}

/** The constraint indexes, in the order the table's statement writes them. */
function constraintIndexes(
  table: string,
  declared: readonly DeclaredColumn[],
  key: readonly number[],
  rowidColumn: number | null,
): IndexShape[] {
  const columnKey = rowidColumn === null && key.length === 1 ? key[0] : null;
  const perColumn = declared.flatMap((column, position) => {
    const origins: IndexOrigin[] = [];
    if (columnKey === position) origins.push("primary-key");
    if (column.isUnique) origins.push("unique");
    return origins.map((origin) => ({ origin, keys: [position] }));
  });
  const composite =
    key.length > 1 ? [{ origin: "primary-key" as const, keys: key }] : [];
  return [...perColumn, ...composite].map((constraint, position) => ({
    name: constraintIndexName(table, position + 1),
    table,
    keys: constraint.keys.map((column) => ({
      kind: "column" as const,
      column,
    })),
    isUnique: true,
    origin: constraint.origin,
  }));
}
