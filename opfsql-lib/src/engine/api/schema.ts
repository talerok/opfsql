import type { SqlType, Value } from "../../values/index.js";

// What `schema()` returns: the tables and indexes a session sees, described
// for people and tools, without the storage details behind them.

export interface Schema {
  readonly tables: readonly TableInfo[];
}

export interface TableInfo {
  readonly name: string;
  readonly columns: readonly ColumnInfo[];
  /** Indexes created with CREATE INDEX; primary keys show on their columns. */
  readonly indexes: readonly IndexInfo[];
}

export interface ColumnInfo {
  readonly name: string;
  /**
   * The column's type: INTEGER, REAL, TEXT, BLOB, BOOLEAN, JSON or ANY — a
   * column declared VARCHAR(20) is TEXT, one declared BIGINT is INTEGER.
   */
  readonly type: SqlType;
  readonly nullable: boolean;
  readonly primaryKey: boolean;
  readonly unique: boolean;
  readonly autoIncrement: boolean;
  readonly defaultValue: Value;
}

export interface IndexInfo {
  readonly name: string;
  readonly unique: boolean;
  /** One SQL expression per key part: "name", "lower(email)", … */
  readonly expressions: readonly string[];
}
