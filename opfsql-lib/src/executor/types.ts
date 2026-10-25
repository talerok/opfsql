import type { ColumnBinding } from "../sql/binder/types.js";
import type { IndexDef, TableSchema } from "../types.js";
import type { Value } from "../values/index.js";

export type { Value } from "../values/index.js";
export type Tuple = Value[];

export interface SyncPhysicalOperator {
  getLayout(): ColumnBinding[];
  next(): Tuple[] | null;
  reset(): void;
}

export interface CTECacheEntry {
  tuples: Tuple[];
  layout: ColumnBinding[];
}

export interface ExecuteResult {
  /** The result rows, values in column order; outputColumnNames names them. */
  rows: Tuple[];
  rowsAffected: number;
  catalogChanges: CatalogChange[];
}

export type CatalogChange =
  | { type: "CREATE_TABLE"; schema: TableSchema }
  | { type: "DROP_TABLE"; name: string; schema: TableSchema }
  | {
      type: "ALTER_TABLE";
      name: string;
      before: TableSchema;
      after: TableSchema;
    }
  | { type: "CREATE_INDEX"; index: IndexDef }
  | { type: "DROP_INDEX"; name: string; index: IndexDef };
