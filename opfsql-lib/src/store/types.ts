export type {
  LogicalType,
  JsonValue,
  Value,
  ColumnDef,
  TableSchema,
  IndexDef,
  CatalogData,
  RowId,
  Row,
} from '../types.js';

import type { IndexDef, Row, RowId, TableSchema } from '../types.js';

// ---------------------------------------------------------------------------
// Catalog interface
// ---------------------------------------------------------------------------

export interface ICatalog {
  hasTable(name: string): boolean;
  getTable(name: string): TableSchema | undefined;
  addTable(schema: TableSchema): void;
  removeTable(name: string): void;
  updateTable(schema: TableSchema): void;
  getAllTables(): TableSchema[];
  hasIndex(name: string): boolean;
  getIndex(name: string): IndexDef | undefined;
  getTableIndexes(tableName: string): IndexDef[];
  addIndex(index: IndexDef): void;
  removeIndex(name: string): void;
  serialize(): import('../types.js').CatalogData;
}

// ---------------------------------------------------------------------------
// Sync storage backend (load-on-open, random-access reads, flush-on-commit)
// ---------------------------------------------------------------------------

export interface SyncIStorage {
  open(): Promise<void>;
  close(): void;
  get<T>(key: string): T | null;
  putMany(entries: Array<[string, unknown]>): void;
  getAllKeys(prefix: string): string[];
}

// ---------------------------------------------------------------------------
// Sync KV store — WAL + LRU cache over SyncIStorage
// ---------------------------------------------------------------------------

export interface SyncIKVStore {
  readKey<T>(key: string): T | null;
  getAllKeys(prefix: string): string[];
  writeKey(key: string, value: unknown): void;
  deleteKey(key: string): void;
  commit(): void;
  rollback(): void;
}

// ---------------------------------------------------------------------------
// Sync row / index manager interfaces
// ---------------------------------------------------------------------------

export interface SyncIRowManager {
  prepareInsert(tableId: string, row: Row): RowId;
  prepareUpdate(tableId: string, rowId: RowId, row: Row): RowId;
  prepareDelete(tableId: string, rowId: RowId): void;
  scanTable(tableId: string): Iterable<{ rowId: RowId; row: Row }>;
  readRow(tableId: string, rowId: RowId): Row | null;
  deleteTableData(tableId: string): void;
}

export interface SyncIIndexManager {
  insert(indexName: string, key: IndexKey, rowId: RowId): void;
  delete(indexName: string, key: IndexKey, rowId: RowId): void;
  search(indexName: string, predicates: SearchPredicate[], totalColumns?: number): RowId[];
  bulkLoad(indexName: string, entries: Array<{ key: IndexKey; rowId: RowId }>, unique: boolean): void;
  dropIndex(indexName: string): void;
}

// ---------------------------------------------------------------------------
// Index key types
// ---------------------------------------------------------------------------

export type IndexKeyValue = string | number | boolean | null;
export type IndexKey = IndexKeyValue[];

export interface SearchPredicate {
  columnPosition: number;
  comparisonType: 'EQUAL' | 'LESS' | 'GREATER' | 'LESS_EQUAL' | 'GREATER_EQUAL';
  value: string | number | boolean | null;
}
