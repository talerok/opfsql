import type { StaticType } from "../../functions/index.js";
import type {
  Column,
  Index,
  IndexShape,
  Table,
  TableShape,
} from "../../schema/index.js";
import type { Expression } from "./expression.js";

// ---------------------------------------------------------------------------
// Bound statements (docs/planning.md, Bound statements). `Plan` is logical
// as the binder makes them and physical once the optimizer has planned the
// plans inside; DDL holds no plan and passes through unchanged.
// ---------------------------------------------------------------------------

export interface OutputColumn {
  readonly name: string;
  readonly type: StaticType;
}

export interface Query<Plan> {
  readonly kind: "query";
  readonly plan: Plan;
  readonly columns: readonly OutputColumn[];
  readonly parameterCount: number;
}

export interface Assignment<Plan> {
  /** The position of the column set. */
  readonly column: number;
  readonly value: Expression<Plan>;
}

/**
 * What an INSERT does with a row that breaks a unique constraint: nothing,
 * or update the row in its way. `targetIndex` binds that row and
 * `excludedIndex` the row that was refused (`excluded.x`), each as every
 * column of the table in column order, column index = position, no rowid.
 */
export type ConflictAction<Plan> =
  | { readonly kind: "nothing" }
  | {
      readonly kind: "update";
      readonly targetIndex: number;
      readonly excludedIndex: number;
      readonly assignments: readonly Assignment<Plan>[];
      readonly condition: Expression<Plan> | null;
    };

export interface OnConflict<Plan> {
  /** The constraint it handles: its index, the rowid, or null for any. */
  readonly target: Index | "rowid" | null;
  readonly action: ConflictAction<Plan>;
}

export interface Insert<Plan> {
  readonly kind: "insert";
  readonly table: Table;
  /** The columns the source fills, in its column order; others get defaults. */
  readonly columns: readonly number[];
  readonly source: Plan;
  /**
   * True when `source`, or a subquery anywhere in it, reads a table the
   * insert writes: the target, or opfsql_sequence when the target has
   * AUTOINCREMENT. The executor then reads all of it before inserting, so
   * `INSERT INTO t SELECT … FROM t` never reads its own rows; otherwise it
   * inserts each row as the source produces it.
   */
  readonly readsTarget: boolean;
  readonly onConflict: OnConflict<Plan> | null;
  readonly parameterCount: number;
}

/**
 * `source` produces the target rows: every column of `table` in column
 * order, then ROWID_COLUMN, bound to `tableIndex`. No pass may prune those
 * columns: the update needs every value. Assignments are computed from them.
 */
export interface Update<Plan> {
  readonly kind: "update";
  readonly table: Table;
  readonly tableIndex: number;
  readonly source: Plan;
  readonly assignments: readonly Assignment<Plan>[];
  readonly parameterCount: number;
}

/**
 * `source` produces the rows to delete, ROWID_COLUMN among their columns,
 * bound to `tableIndex`: the delete reads that one. A logical source is
 * every column then the rowid, as for Update, under the WHERE that reads
 * them; once the WHERE is in a scan's residual, the optimizer may narrow a
 * physical source to the rowid alone.
 */
export interface Delete<Plan> {
  readonly kind: "delete";
  readonly table: Table;
  readonly tableIndex: number;
  readonly source: Plan;
  readonly parameterCount: number;
}

export interface CreateTable {
  readonly kind: "create-table";
  readonly table: TableShape;
  /**
   * The indexes its PRIMARY KEY and UNIQUE constraints need, named
   * constraintIndexName(table, n) in order: each column's constraints in
   * column order (PRIMARY KEY before UNIQUE), then a table PRIMARY KEY.
   */
  readonly constraints: readonly IndexShape[];
  readonly isIfNotExists: boolean;
}

export interface CreateIndex {
  readonly kind: "create-index";
  readonly index: IndexShape;
  readonly isIfNotExists: boolean;
}

export interface Drop {
  readonly kind: "drop";
  readonly object: "table" | "index";
  readonly name: string;
  readonly isIfExists: boolean;
}

export type AlterAction =
  | { readonly kind: "add-column"; readonly column: Column }
  | { readonly kind: "drop-column"; readonly column: number };

export interface AlterTable {
  readonly kind: "alter-table";
  readonly table: Table;
  readonly action: AlterAction;
}

export type Statement<Plan> =
  | Query<Plan>
  | Insert<Plan>
  | Update<Plan>
  | Delete<Plan>
  | CreateTable
  | CreateIndex
  | Drop
  | AlterTable;
