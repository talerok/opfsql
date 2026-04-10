import type {
  TableSchema,
  IndexDef,
  Row,
} from '../store/types.js';
import type { ColumnBinding } from '../binder/types.js';

// ---------------------------------------------------------------------------
// Core value types
// ---------------------------------------------------------------------------

export type Value = string | number | boolean | null;
export type Tuple = Value[];

// ---------------------------------------------------------------------------
// Physical operator interface (Volcano pull model)
// ---------------------------------------------------------------------------

export interface PhysicalOperator {
  /** Column layout — position in layout = position in output tuple */
  getLayout(): ColumnBinding[];
  /** Next batch of tuples, null = exhausted */
  next(): Promise<Tuple[] | null>;
  /** Reset to beginning (used by nested loop join) */
  reset(): Promise<void>;
}

// ---------------------------------------------------------------------------
// CTE cache
// ---------------------------------------------------------------------------

export interface CTECacheEntry {
  tuples: Tuple[];
  layout: ColumnBinding[];
}

// ---------------------------------------------------------------------------
// Execution result
// ---------------------------------------------------------------------------

export interface ExecuteResult {
  rows: Row[];
  rowsAffected: number;
  catalogChanges: CatalogChange[];
}

export type CatalogChange =
  | { type: 'CREATE_TABLE'; schema: TableSchema }
  | { type: 'DROP_TABLE'; name: string; schema: TableSchema }
  | {
      type: 'ALTER_TABLE';
      name: string;
      before: TableSchema;
      after: TableSchema;
    }
  | { type: 'CREATE_INDEX'; index: IndexDef }
  | { type: 'DROP_INDEX'; name: string; index: IndexDef };
