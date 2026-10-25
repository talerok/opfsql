import type { Value } from "../../values/index.js";

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
  /** The declared type: "INTEGER", "TEXT", … */
  readonly type: string;
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
