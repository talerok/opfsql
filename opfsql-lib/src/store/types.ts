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
import type { EncodedPage } from "./page-codec.js";

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
  /** Largest encoded page payload, in bytes. Valid after open(). */
  readonly pageCapacity: number;
  /**
   * Committed contents of a page as of `snapshot`, a commit position from
   * latestCommit() (default: the latest). Backends without page versions
   * ignore it.
   */
  readPage<T>(pageNo: number, snapshot?: number): T | null;
  getNextPageId(): number;
  /** Position of the latest commit, for backends that keep page versions. */
  latestCommit?(): number;
  /** Oldest snapshot open in this engine (null: none), for backends that keep page versions. */
  setOldestSnapshot?(oldest: number | null): void;
  /** False if a snapshot taken at `snapshot` may already be outdated by another tab's checkpoint. */
  isSnapshotSafe?(snapshot: number): boolean;
  /** Releases snapshots held by tabs that no longer exist. */
  releaseAbandonedSnapshots?(): Promise<void>;
  /**
   * Persist a batch of pages together with the new nextPageId. Rejects the
   * whole batch with PageOverflowError before writing if any page exceeds
   * pageCapacity. Crash atomicity is backend-specific: WalStorage is
   * all-or-nothing (durable once commit() returns, invisible if it throws);
   * OPFSSyncStorage writes in place and relies on running under WalStorage.
   */
  commit(pages: ReadonlyMap<number, EncodedPage>, nextPageId: number): void;
  /** Shrink backing file to match nextPageId. Safe only after commit(). */
  truncateToSize?(): void;
  /** Catch up with changes made by other writers. Returns set of changed page numbers, or null if nothing changed. */
  catchUp?(): Set<number> | null;
  /** Bring the main file up to `upTo`, no newer than the oldest open snapshot. */
  checkpoint?(upTo: number): void;
  /** True when the backend wants checkpoint() to be called. */
  needsCheckpoint?(): boolean;
}

// ---------------------------------------------------------------------------
// Sync page store — page allocator over SyncIPageStorage
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
  /** Reserves and returns the next value for the table's AUTOINCREMENT column. */
  nextAutoIncrement(tableId: string): number;
  /** Records an explicitly inserted AUTOINCREMENT value, so it is never reissued. */
  observeAutoIncrement(tableId: string, value: number): void;
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
