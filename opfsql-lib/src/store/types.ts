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
// Sync page storage backend (page-based I/O, no string-keyed index)
// ---------------------------------------------------------------------------

export interface SyncIPageStorage {
  open(): Promise<void>;
  close(): void;
  readPage<T>(pageNo: number): T | null;
  writePage(pageNo: number, value: unknown): void;
  getNextPageId(): number;
  writeHeader(nextPageId: number): void;
  flush(): void;
}

// ---------------------------------------------------------------------------
// Sync page store — WAL + LRU cache + page allocator over SyncIPageStorage
// ---------------------------------------------------------------------------

export interface SyncIPageStore {
  readPage<T>(pageNo: number): T | null;
  writePage(pageNo: number, value: unknown): void;
  allocPage(): number;
  freePage(pageNo: number): void;
  commit(): void;
  rollback(): void;
}

// ---------------------------------------------------------------------------
// Sync row / index manager interfaces
// ---------------------------------------------------------------------------

export interface SyncIRowManager {
  createTable(): number;
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
  bulkLoad(indexName: string, entries: Array<{ key: IndexKey; rowId: RowId }>, unique: boolean): number;
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
