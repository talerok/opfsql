import type { ColumnBinding } from "../binder/types.js";
import type { Row, Value as StoreValue } from "../store/types.js";

export type Value = StoreValue;
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

import type { IndexDef, TableSchema } from "../store/types.js";

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
