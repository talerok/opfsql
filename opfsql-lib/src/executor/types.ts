import type { ColumnBinding } from "../binder/types.js";
import type { IndexDef, Row, TableSchema, Value as BaseValue } from "../types.js";

export type Value = BaseValue;
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
  rows: Row[];
  rowsAffected: number;
  catalogChanges: CatalogChange[];
  catalogDirty?: boolean;
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
