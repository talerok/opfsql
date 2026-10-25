import type { StaticType } from "../../functions/index.js";
import type { Column, Index, Table } from "../../schema/index.js";
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
 * or update the row in its way. `targetIndex` binds that row's columns and
 * `excludedIndex` the row that was refused (`excluded.x`).
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
  readonly onConflict: OnConflict<Plan> | null;
  readonly parameterCount: number;
}

/**
 * `source` produces the target rows: every column of `table`, then the
 * rowid, bound to `tableIndex`. Assignments are computed from those rows.
 */
export interface Update<Plan> {
  readonly kind: "update";
  readonly table: Table;
  readonly tableIndex: number;
  readonly source: Plan;
  readonly assignments: readonly Assignment<Plan>[];
  readonly parameterCount: number;
}

/** `source` produces the rows to delete, their rowid last, as for Update. */
export interface Delete<Plan> {
  readonly kind: "delete";
  readonly table: Table;
  readonly tableIndex: number;
  readonly source: Plan;
  readonly parameterCount: number;
}

/** A table or index as a statement defines it, before it has pages. */
export type TableDefinition = Omit<Table, "rootPage">;
export type IndexDefinition = Omit<Index, "rootPage">;

export interface CreateTable {
  readonly kind: "create-table";
  readonly table: TableDefinition;
  /** The indexes its PRIMARY KEY and UNIQUE constraints need. */
  readonly constraints: readonly IndexDefinition[];
  readonly isIfNotExists: boolean;
}

export interface CreateIndex {
  readonly kind: "create-index";
  readonly index: IndexDefinition;
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
