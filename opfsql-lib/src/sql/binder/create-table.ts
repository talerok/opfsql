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

/**
 * The primary key's columns, written with a column or after them; a table
 * has one at most, as SQLite says.
 */
function primaryKey(
  statement: CreateTableStatement,
  declared: readonly DeclaredColumn[],
): number[] {
  const columnKeys = declared.reduce(
    (count, column) => count + column.primaryKeys,
    0,
  );
  if (columnKeys + statement.primaryKeys.length > 1) {
    throw new SemanticError(
      `table "${statement.table}" has more than one primary key`,
    );
  }
  const keyColumn = declared.findIndex((column) => column.primaryKeys > 0);
  if (keyColumn !== -1) return [keyColumn];
  return tableKey(statement, declared);
}

/** A table-level PRIMARY KEY's columns, each named once; none without one. */
function tableKey(
  statement: CreateTableStatement,
  declared: readonly DeclaredColumn[],
): number[] {
  const [names = []] = statement.primaryKeys;
  const columns = declared.map(({ column }) => column);
  const key = names.map((name) => columnIn(columns, name, statement.table));
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

/** A constraint before it has its index's name. */
interface Constraint {
  readonly origin: IndexOrigin;
  readonly columns: readonly number[];
}

/** The constraint indexes, in the order the table's statement writes them. */
function constraintIndexes(
  table: string,
  declared: readonly DeclaredColumn[],
  key: readonly number[],
  rowidColumn: number | null,
): IndexShape[] {
  const [keyColumn] = key;
  const isColumnKey = rowidColumn === null && key.length === 1;
  const columnKey = isColumnKey ? keyColumn : undefined;
  const constraints = [
    ...declared.flatMap((column, position) =>
      columnConstraints(column, position, columnKey),
    ),
    ...(key.length > 1 ? [primaryKeyOn(key)] : []),
  ];
  return constraints.map((constraint, position) =>
    constraintIndex(table, constraint, position + 1),
  );
}

/** A column's own constraints: PRIMARY KEY before UNIQUE. */
function columnConstraints(
  column: DeclaredColumn,
  position: number,
  keyColumn: number | undefined,
): Constraint[] {
  const constraints: Constraint[] = [];
  if (keyColumn === position) constraints.push(primaryKeyOn([position]));
  if (column.isUnique)
    constraints.push({ origin: "unique", columns: [position] });
  return constraints;
}

function primaryKeyOn(columns: readonly number[]): Constraint {
  return { origin: "primary-key", columns };
}

function constraintIndex(
  table: string,
  constraint: Constraint,
  ordinal: number,
): IndexShape {
  return {
    name: constraintIndexName(table, ordinal),
    tableName: table,
    keys: constraint.columns.map((column) => ({ kind: "column", column })),
    isUnique: true,
    origin: constraint.origin,
  };
}
