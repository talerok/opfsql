import { ConstraintError } from "../errors.js";
import type { Index, IndexKey, Table } from "../schema/index.js";
import type { IndexReader } from "../storage/index.js";
import { encodeKey } from "../storage/index.js";
import type { Value } from "../values/index.js";
import { rowidsIn } from "./index-rows.js";

// ---------------------------------------------------------------------------
// Index keys of rows. A key part is a column or an expression; expressions
// are SQL text in the schema, and running one takes the parser and binder,
// which sit above this layer. So whoever opens a table passes a compiler.
// ---------------------------------------------------------------------------

/** An expression over a row of a table, ready to run. */
export type RowExpression = (row: readonly Value[]) => Value;

/** Compiles an index key expression of `table` from its SQL text. */
export type CompileExpression = (sql: string, table: Table) => RowExpression;

/** An index, and how to compute a row's key in it. */
export interface KeyedIndex {
  readonly index: Index;
  /** The key's values for a row, in key order, without the rowid. */
  readonly keyOf: (row: readonly Value[]) => Value[];
}

export function keyedIndex(
  index: Index,
  table: Table,
  compile: CompileExpression,
): KeyedIndex {
  const parts = index.keys.map((key) => keyPart(key, table, compile));
  return { index, keyOf: (row) => parts.map((part) => part(row)) };
}

/** The entry a row has in the index: its key, then its rowid. */
export function entryOf(
  keyed: KeyedIndex,
  row: readonly Value[],
  rowid: number,
): Uint8Array {
  return encodeKey([...keyed.keyOf(row), rowid]);
}

function keyPart(
  key: IndexKey,
  table: Table,
  compile: CompileExpression,
): RowExpression {
  switch (key.kind) {
    case "column": {
      const { column } = key;
      return (row) => row[column] ?? null;
    }
    case "expression": {
      return compile(key.sql, table);
    }
  }
}

/**
 * The rowids of the entries with exactly `key`. A key with a NULL has none:
 * NULLs are distinct, so it never conflicts, as in SQLite.
 */
export function rowidsWithKey(
  entries: IndexReader,
  key: readonly Value[],
): Generator<number> {
  return rowidsIn(entries, { equal: key, range: null }, "ascending");
}

/** The error for a row whose key in a unique index another row already has. */
export function uniqueViolation(table: Table, index: Index): ConstraintError {
  const kind = index.origin === "primary-key" ? "primary-key" : "unique";
  const columns = keyColumnNames(table, index);
  return new ConstraintError(kind, table.name, columns, index.name);
}

/** The columns an index covers; none if any key part is an expression. */
function keyColumnNames(table: Table, index: Index): string[] {
  const names: string[] = [];
  for (const key of index.keys) {
    if (key.kind === "expression") return [];
    const column = table.columns[key.column];
    if (column === undefined) return [];
    names.push(column.name);
  }
  return names;
}
