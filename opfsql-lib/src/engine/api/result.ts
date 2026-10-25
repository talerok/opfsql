import { MisuseError } from "../../errors.js";
import type { Value } from "../../values/index.js";

/** One row of a result: its values in the order of the result's columns. */
export type ResultRow = readonly Value[];

/** A result as plain data: what crosses the worker boundary. */
export interface ResultData {
  readonly columns: readonly string[];
  readonly rows: readonly ResultRow[];
  readonly changes: number;
  readonly lastInsertRowid: number | null;
}

/** What one statement returned. */
export class Result implements ResultData {
  /** One name per column, in order, also when there are no rows; names may repeat. */
  readonly columns: readonly string[];
  /** One array per row, its values in the order of `columns`. */
  readonly rows: readonly ResultRow[];
  /** Rows the statement inserted, updated or deleted; 0 for other statements. */
  readonly changes: number;
  /** The rowid of the last row this statement inserted, or null if none. */
  readonly lastInsertRowid: number | null;

  constructor(data: ResultData) {
    this.columns = data.columns;
    this.rows = data.rows;
    this.changes = data.changes;
    this.lastInsertRowid = data.lastInsertRowid;
  }

  /**
   * The rows as objects keyed by column name. Fails with MISUSE when two
   * columns share a name, since one would hide the other: alias them, or
   * read `rows`.
   */
  objects<T extends Record<string, Value> = Record<string, Value>>(): T[] {
    const repeated = this.columns.find(
      (name, index) => this.columns.indexOf(name) !== index,
    );
    if (repeated !== undefined) {
      throw new MisuseError(
        `column name "${repeated}" appears more than once; alias the columns or read rows`,
      );
    }
    return this.rows.map((row) => {
      const object: Record<string, Value> = {};
      for (const [index, name] of this.columns.entries()) {
        object[name] = row[index] ?? null;
      }
      return object as T;
    });
  }

  /** The plain data, for structured cloning. */
  toData(): ResultData {
    return {
      columns: this.columns,
      rows: this.rows,
      changes: this.changes,
      lastInsertRowid: this.lastInsertRowid,
    };
  }
}

/** The result of a statement that returns no rows and changes none. */
export const emptyResult: Result = new Result({
  columns: [],
  rows: [],
  changes: 0,
  lastInsertRowid: null,
});
