import type { SqlType, Value } from "../values/index.js";

// ---------------------------------------------------------------------------
// The schema model: what tables and indexes exist and what they hold. It is
// plain readonly data, built from the schema table of one snapshot and never
// changed; a schema change builds a new one.
//
// Expressions appear here as SQL text. Turning them into something that runs
// takes the parser and the binder, which sit above this module; whoever needs
// to evaluate one compiles it (docs/architecture.md, Modules).
// ---------------------------------------------------------------------------

export interface Column {
  readonly name: string;
  readonly type: SqlType;
  readonly isNotNull: boolean;
  /** The value an INSERT that leaves the column out stores. */
  readonly defaultValue: Value;
}

export interface Table {
  readonly name: string;
  readonly columns: readonly Column[];
  /** The root page of the table's B-tree, which never moves. */
  readonly rootPage: number;
  /**
   * The INTEGER PRIMARY KEY column, whose value is the row's rowid (docs/
   * semantics.md, Primary keys); null when the rowid is not a column.
   */
  readonly rowidColumn: number | null;
  /** Rowids are never reused: the table has an AUTOINCREMENT column. */
  readonly isAutoincrement: boolean;
}

/** One part of an index key: a column of the table, or an expression over its columns. */
export type IndexKey =
  | { readonly kind: "column"; readonly column: number }
  | { readonly kind: "expression"; readonly sql: string };

/** Why an index exists. */
export type IndexOrigin =
  /** A CREATE INDEX statement. */
  | "statement"
  /** A PRIMARY KEY that is not the rowid: unique, over NOT NULL columns. */
  | "primary-key"
  /** A UNIQUE constraint. */
  | "unique";

export interface Index {
  readonly name: string;
  /** The name of the table it indexes. */
  readonly table: string;
  readonly keys: readonly IndexKey[];
  readonly isUnique: boolean;
  /** The root page of the index's B-tree, which never moves. */
  readonly rootPage: number;
  /** An index made for a constraint is created and dropped with its table. */
  readonly origin: IndexOrigin;
}
