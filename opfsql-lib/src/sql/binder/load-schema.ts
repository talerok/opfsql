import { CorruptError } from "../../errors.js";
import {
  nameKey,
  Schema,
  type Index,
  type IndexShape,
  type Table,
} from "../../schema/index.js";
import { parse, StatementType, type Statement } from "../parser/index.js";
import { asCorruption } from "./corruption.js";
import { indexDefinition } from "./create-index.js";
import { tableDefinition } from "./create-table.js";
import { bindQuery } from "./query.js";
import { exactlyOne } from "./one.js";
import { BindContext } from "./scope.js";

// ---------------------------------------------------------------------------
// The schema of a snapshot from the rows of opfsql_schema (docs/planning.md,
// Schema text): each table's CREATE TABLE and each index's CREATE INDEX,
// parsed and bound back into the model they were generated from. A table's
// constraint indexes have no statement of their own; the table's names them,
// and their rows give their root pages. Stored statements may use the
// reserved opfsql_ names, which user statements may not. A row that does
// not load is a CorruptError naming the object.
// ---------------------------------------------------------------------------

/** A row of the schema table. */
export interface SchemaRow {
  readonly type: "table" | "index";
  readonly name: string;
  /** The table an index belongs to; a table's own name for a table. */
  readonly tableName: string;
  readonly rootPage: number;
  /** The CREATE statement; null for an index made by a constraint. */
  readonly sql: string | null;
}

/** A table loaded, and the constraint indexes its statement describes. */
interface LoadedTable {
  readonly table: Table;
  readonly constraints: readonly IndexShape[];
}

export function loadSchema(
  version: number,
  rows: readonly SchemaRow[],
): Schema {
  const loaded = rows
    .filter((row) => row.type === "table")
    .map((row) => loadTable(row));
  const tables = new Map(
    loaded.map((item) => [nameKey(item.table.name), item]),
  );
  const indexes = rows
    .filter((row) => row.type === "index")
    .map((row) => loadIndex(row, tables));
  for (const item of loaded) checkConstraintRows(item, indexes);
  return new Schema(
    version,
    loaded.map((item) => item.table),
    indexes,
  );
}

function loadTable(row: SchemaRow): LoadedTable {
  const statement = storedStatement(row, StatementType.CREATE_TABLE_STATEMENT);
  const { table, constraints } = loading(row, () => tableDefinition(statement));
  checkName(row, table.name);
  return { table: { ...table, rootPage: row.rootPage }, constraints };
}

function loadIndex(
  row: SchemaRow,
  tables: ReadonlyMap<string, LoadedTable>,
): Index {
  const owner = tables.get(nameKey(row.tableName));
  if (owner === undefined) {
    throw corrupt(row, `its table ${row.tableName} has no row`);
  }
  // A constraint's index has no statement: its table's statement names it.
  const isConstraint = row.sql === null;
  if (isConstraint) return loadConstraintIndex(row, owner);
  return loadStatementIndex(row, owner);
}

/** A constraint index: its table's statement describes it, the row gives its pages. */
function loadConstraintIndex(row: SchemaRow, owner: LoadedTable): Index {
  const constraint = owner.constraints.find(
    (index) => nameKey(index.name) === nameKey(row.name),
  );
  if (constraint === undefined) {
    throw corrupt(
      row,
      `it has no statement and no constraint of ${owner.table.name} is named so`,
    );
  }
  return { ...constraint, rootPage: row.rootPage };
}

/** An index a CREATE INDEX made, bound again over its table. */
function loadStatementIndex(row: SchemaRow, owner: LoadedTable): Index {
  const statement = storedStatement(row, StatementType.CREATE_INDEX_STATEMENT);
  const context = new BindContext(Schema.empty, bindQuery);
  const index = loading(row, () =>
    indexDefinition(statement, owner.table, context),
  );
  checkName(row, index.name);
  if (nameKey(statement.table) !== nameKey(row.tableName)) {
    throw corrupt(row, `its statement indexes ${statement.table}`);
  }
  return { ...index, rootPage: row.rootPage };
}

/** Every constraint index a table's statement names has a row of its own. */
function checkConstraintRows(
  item: LoadedTable,
  indexes: readonly Index[],
): void {
  for (const constraint of item.constraints) {
    const key = nameKey(constraint.name);
    if (indexes.every((index) => nameKey(index.name) !== key)) {
      throw new CorruptError(
        `table ${item.table.name} has a constraint index ${constraint.name} with no row in opfsql_schema`,
      );
    }
  }
}

/** The one statement of `type` the row stores. */
function storedStatement<Type extends StatementType>(
  row: SchemaRow,
  type: Type,
): Extract<Statement, { type: Type }> {
  const { sql } = row;
  if (sql === null) throw corrupt(row, "it has no statement");
  const wrong = (): CorruptError =>
    corrupt(row, `its statement is not one ${type}: ${sql}`);
  const statement = exactlyOne(
    loading(row, () => parse(sql)),
    wrong,
  );
  if (!isOfType(statement, type)) throw wrong();
  return statement;
}

function isOfType<Type extends StatementType>(
  statement: Statement,
  type: Type,
): statement is Extract<Statement, { type: Type }> {
  return statement.type === type;
}

function checkName(row: SchemaRow, name: string): void {
  if (nameKey(name) !== nameKey(row.name)) {
    throw corrupt(row, `its statement names ${name}`);
  }
}

/** `load()`, its parse and bind errors reported as the row's corruption. */
function loading<T>(row: SchemaRow, load: () => T): T {
  return asCorruption(load, (problem) => describeCorruption(row, problem));
}

function corrupt(row: SchemaRow, problem: string): CorruptError {
  return new CorruptError(describeCorruption(row, problem));
}

function describeCorruption(row: SchemaRow, problem: string): string {
  return `the schema of ${row.type} ${row.name} does not load: ${problem}`;
}
