import {
  type Column,
  type Index,
  type IndexKey,
  isReservedName,
  type Schema as SchemaModel,
  type Table,
} from "../schema/index.js";
import type { ColumnInfo, IndexInfo, Schema, TableInfo } from "./api/index.js";

// What schema() returns: the schema model as callers read it, without root
// pages or versions. opfsql's own tables are left out, and so are the indexes
// of PRIMARY KEY and UNIQUE constraints, which their columns' `primaryKey`
// and `unique` show: the indexes listed are those CREATE INDEX made.

export function describeSchema(model: SchemaModel): Schema {
  const tables = model.tables.filter((table) => !isReservedName(table.name));
  return { tables: tables.map((table) => describeTable(model, table)) };
}

function describeTable(model: SchemaModel, table: Table): TableInfo {
  const indexes = model.indexesOf(table.name);
  return {
    name: table.name,
    columns: table.columns.map((column, position) =>
      describeColumn(column, position, { table, indexes }),
    ),
    indexes: indexes
      .filter((index) => index.origin === "statement")
      .map((index) => describeIndex(table, index)),
  };
}

/** A table and its indexes. */
interface TableIndexes {
  readonly table: Table;
  readonly indexes: readonly Index[];
}

function describeColumn(
  column: Column,
  position: number,
  { table, indexes }: TableIndexes,
): ColumnInfo {
  const { name, type, isNotNull, defaultValue } = column;
  const isRowid = table.rowidColumn === position;
  const isKey =
    isRowid || indexes.some((index) => isPrimaryKey(index, position));
  return {
    name,
    type,
    nullable: !isNotNull && !isKey,
    primaryKey: isKey,
    unique: indexes.some((index) => isUniqueColumn(index, position)),
    autoIncrement: isRowid && table.isAutoincrement,
    defaultValue,
  };
}

function isPrimaryKey(index: Index, position: number): boolean {
  return (
    index.origin === "primary-key" &&
    index.keys.some((key) => isColumn(key, position))
  );
}

/** A UNIQUE constraint on this column alone. */
function isUniqueColumn(index: Index, position: number): boolean {
  const [key, ...others] = index.keys;
  const isOnlyKey = key !== undefined && others.length === 0;
  return index.origin === "unique" && isOnlyKey && isColumn(key, position);
}

function isColumn(key: IndexKey, position: number): boolean {
  return key.kind === "column" && key.column === position;
}

function describeIndex(table: Table, index: Index): IndexInfo {
  return {
    name: index.name,
    unique: index.isUnique,
    expressions: index.keys.map((key) => keyText(table, key)),
  };
}

/** A key part as SQL: its column's name, or its expression's canonical text. */
function keyText(table: Table, key: IndexKey): string {
  if (key.kind === "expression") return key.sql;
  return table.columns[key.column]?.name ?? "";
}
