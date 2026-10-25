import { InternalError } from "../errors.js";
import type { SqlType, Value } from "../values/index.js";
import { bytesToHex, canonicalJson } from "../values/index.js";
import type { Column, Index, IndexKey, Table } from "./model.js";

// ---------------------------------------------------------------------------
// The CREATE statements opfsql_schema stores, generated from the model
// (docs/planning.md, Schema text). Parsing and binding one gives back the
// model it came from; a schema change builds a new model and writes its
// statement, never edits the text.
// ---------------------------------------------------------------------------

/** A table as the statement describes it: no pages needed. */
export type TableShape = Omit<Table, "rootPage">;

/** An index as the statement describes it. */
export type IndexShape = Omit<Index, "rootPage">;

const AUTOINDEX_PREFIX = "opfsql_autoindex_";

/** The name of the index a table's `ordinal`-th constraint needs, from 1. */
export function constraintIndexName(table: string, ordinal: number): string {
  return `${AUTOINDEX_PREFIX}${table}_${String(ordinal)}`;
}

/**
 * CREATE TABLE for `table`, with `constraints`, the indexes of its PRIMARY
 * KEY and UNIQUE constraints.
 */
export function createTableSql(
  table: TableShape,
  constraints: readonly IndexShape[],
): string {
  const columns = table.columns.map((column, position) =>
    columnSql(table, constraints, column, position),
  );
  const parts = [...columns];
  const compositeKey = constraints.find(
    (index) => index.origin === "primary-key" && index.keys.length > 1,
  );
  if (compositeKey !== undefined) {
    parts.push(`PRIMARY KEY (${keyList(table, compositeKey.keys)})`);
  }
  return `CREATE TABLE ${quoteName(table.name)} (${parts.join(", ")})`;
}

/** CREATE INDEX for an index made by a statement. */
export function createIndexSql(index: IndexShape, table: TableShape): string {
  const unique = index.isUnique ? "UNIQUE " : "";
  const name = quoteName(index.name);
  const on = quoteName(table.name);
  return `CREATE ${unique}INDEX ${name} ON ${on} (${keyList(table, index.keys)})`;
}

/** A name as a quoted identifier: always valid, whatever words are keywords. */
export function quoteName(name: string): string {
  return `"${name.replaceAll('"', '""')}"`;
}

/** A value as a SQL literal that reads back as the same value. */
export function literalSql(value: Value): string {
  if (value === null) return "NULL";
  if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
  if (typeof value === "number") return numberSql(value);
  if (typeof value === "string") return textSql(value);
  if (value instanceof Uint8Array) return `X'${bytesToHex(value)}'`;
  throw new InternalError("a JSON value has no SQL literal outside JSON text");
}

/** A column default: a JSON column's value is written as JSON text. */
export function defaultSql(value: Value, type: SqlType): string {
  const isJson = type === "JSON";
  if (!isJson || value === null || value instanceof Uint8Array) {
    return literalSql(value);
  }
  // A JSON column reads text as JSON text, so its values are written so.
  return textSql(canonicalJson(value));
}

function columnSql(
  table: TableShape,
  constraints: readonly IndexShape[],
  column: Column,
  position: number,
): string {
  const parts = [quoteName(column.name), column.type];
  if (position === table.rowidColumn) parts.push(rowidKeySql(table));
  const origin = singleColumnConstraint(constraints, position);
  if (origin !== null)
    parts.push(origin === "unique" ? "UNIQUE" : "PRIMARY KEY");
  if (column.isNotNull) parts.push("NOT NULL");
  if (column.defaultValue !== null) {
    parts.push(`DEFAULT ${defaultSql(column.defaultValue, column.type)}`);
  }
  return parts.join(" ");
}

function rowidKeySql(table: TableShape): string {
  return table.isAutoincrement ? "PRIMARY KEY AUTOINCREMENT" : "PRIMARY KEY";
}

/** The constraint on column `position` alone, if any: written with the column. */
function singleColumnConstraint(
  constraints: readonly IndexShape[],
  position: number,
): Index["origin"] | null {
  const constraint = constraints.find((index) => isOnColumn(index, position));
  return constraint?.origin ?? null;
}

function isOnColumn(index: IndexShape, position: number): boolean {
  const [first] = index.keys;
  const isSingle = index.keys.length === 1;
  return isSingle && first?.kind === "column" && first.column === position;
}

function keyList(table: TableShape, keys: readonly IndexKey[]): string {
  return keys.map((key) => keySql(table, key)).join(", ");
}

function keySql(table: TableShape, key: IndexKey): string {
  if (key.kind === "expression") return key.sql;
  const column = table.columns[key.column];
  if (column === undefined) {
    throw new InternalError(
      `table ${table.name} has no column ${String(key.column)}`,
    );
  }
  return quoteName(column.name);
}

/** SQL has no literal for infinity; 9e999 overflows to it, as in SQLite. */
function numberSql(value: number): string {
  if (value === Infinity) return "9e999";
  if (value === -Infinity) return "-9e999";
  return String(value);
}

function textSql(text: string): string {
  return `'${text.replaceAll("'", "''")}'`;
}
