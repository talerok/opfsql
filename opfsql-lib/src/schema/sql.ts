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
 * KEY and UNIQUE constraints. Anything a CREATE TABLE cannot say — an
 * index that is no constraint's, a UNIQUE over several columns, AUTOINCREMENT
 * without a rowid column — is refused rather than lost.
 */
export function createTableSql(
  table: TableShape,
  constraints: readonly IndexShape[],
): string {
  if (table.isAutoincrement && table.rowidColumn === null) {
    throw new InternalError(
      `${table.name} has AUTOINCREMENT but no rowid column`,
    );
  }
  const { byColumn, compositeKey } = orderConstraints(table, constraints);
  const parts = table.columns.map((column, position) =>
    columnSql(table, column, position, byColumn[position] ?? []),
  );
  if (compositeKey !== null) {
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

/** A table's constraints as its statement writes them. */
interface OrderedConstraints {
  /** Per column, the constraints written with it: PRIMARY KEY before UNIQUE. */
  readonly byColumn: readonly (readonly IndexShape[])[];
  /** A PRIMARY KEY over several columns, written after them. */
  readonly compositeKey: IndexShape | null;
}

/**
 * The constraints in the order the statement writes them: each column's in
 * column order, then a table PRIMARY KEY. They must be named in that order
 * (constraintIndexName), since loading the statement matches them by name.
 */
function orderConstraints(
  table: TableShape,
  constraints: readonly IndexShape[],
): OrderedConstraints {
  const byColumn = table.columns.map((_, position) =>
    constraintsOn(constraints, position),
  );
  const composite = constraints.filter(
    (index) => index.origin === "primary-key" && index.keys.length > 1,
  );
  const ordered = [...byColumn.flat(), ...composite];
  if (ordered.length !== constraints.length || composite.length > 1) {
    throw new InternalError(
      `${table.name} has a constraint CREATE TABLE cannot write`,
    );
  }
  for (const [position, index] of ordered.entries()) {
    checkName(table, index, position + 1);
  }
  return { byColumn, compositeKey: composite[0] ?? null };
}

/** The constraints on column `position` alone: PRIMARY KEY, then UNIQUE. */
function constraintsOn(
  constraints: readonly IndexShape[],
  position: number,
): IndexShape[] {
  const onColumn = constraints.filter((index) => isOnColumn(index, position));
  return [
    ...onColumn.filter((index) => index.origin === "primary-key"),
    ...onColumn.filter((index) => index.origin === "unique"),
  ];
}

function checkName(
  table: TableShape,
  index: IndexShape,
  ordinal: number,
): void {
  const expected = constraintIndexName(table.name, ordinal);
  if (index.name !== expected) {
    throw new InternalError(`${index.name} should be named ${expected}`);
  }
}

function columnSql(
  table: TableShape,
  column: Column,
  position: number,
  constraints: readonly IndexShape[],
): string {
  const parts = [quoteName(column.name), column.type];
  if (position === table.rowidColumn) parts.push(rowidKeySql(table));
  for (const { origin } of constraints) parts.push(CONSTRAINT_SQL[origin]);
  if (column.isNotNull) parts.push("NOT NULL");
  if (column.defaultValue !== null) {
    parts.push(`DEFAULT ${defaultSql(column.defaultValue, column.type)}`);
  }
  return parts.join(" ");
}

/** What a constraint index is written as; a statement's index never is. */
const CONSTRAINT_SQL: Record<Index["origin"], string> = {
  "primary-key": "PRIMARY KEY",
  unique: "UNIQUE",
  statement: "",
};

function rowidKeySql(table: TableShape): string {
  return table.isAutoincrement ? "PRIMARY KEY AUTOINCREMENT" : "PRIMARY KEY";
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
