export type {
  CatalogData,
  ColumnDef,
  IndexDef,
  IndexExpression,
  JsonValue,
  LogicalType,
  Row,
  RowId,
  TableSchema,
  Value,
} from "../types.js";
export type { SearchPredicate } from "./index-btree/search-bounds.js";
export type { IndexKey, IndexKeyValue } from "./index-btree/types.js";

import type {
  CatalogData,
  IndexDef,
  Row,
  RowId,
  TableSchema,
} from "../types.js";
import type { SearchPredicate } from "./index-btree/search-bounds.js";
import type { IndexKey } from "./index-btree/types.js";

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
  serialize(): CatalogData;
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
  /** Shrink backing file to match nextPageId. Safe only after flush(). */
  truncateToSize?(): void;
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
  search(indexName: string, predicates: SearchPredicate[]): RowId[];
  bulkLoad(
    indexName: string,
    entries: Array<{ key: IndexKey; rowId: RowId }>,
    unique: boolean,
  ): number;
  dropIndex(indexName: string): void;
  /** Smallest non-null key in the index, or null. */
  first(indexName: string): { key: IndexKey; rowId: RowId } | null;
  /** Largest non-null key in the index, or null. */
  last(indexName: string): { key: IndexKey; rowId: RowId } | null;
}
