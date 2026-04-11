export type LogicalType =
  | "INTEGER"
  | "BIGINT"
  | "REAL"
  | "TEXT"
  | "BLOB"
  | "BOOLEAN"
  | "NULL"
  | "ANY";

export interface ColumnDef {
  name: string;
  type: LogicalType;
  nullable: boolean;
  primaryKey: boolean;
  unique: boolean;
  defaultValue: string | number | boolean | null;
}

export interface TableSchema {
  name: string;
  columns: ColumnDef[];
}

export interface IndexDef {
  name: string;
  tableName: string;
  columns: string[];
  unique: boolean;
}

export interface CatalogData {
  tables: TableSchema[];
  indexes: IndexDef[];
}

/** Logical row identifier — auto-incrementing number, stable across page compaction. */
export type RowId = number;

export type Row = Record<string, string | number | boolean | null>;

export interface Page {
  pageId: number;
  tableId: string;
  /** logicalId → Row data. O(1) read/write/delete. */
  rows: Record<number, Row>;
}

export interface PageMeta {
  lastPageId: number;
  nextRowId: number;
  totalRowCount: number;
  freePageIds: number[];
}

export const PAGE_SIZE = 1024;

// ---------------------------------------------------------------------------
// Storage backend interface
// ---------------------------------------------------------------------------

export interface IStorage {
  open(): Promise<void>;
  close(): void;
  get<T>(key: string): Promise<T | null>;
  put(key: string, value: unknown): Promise<void>;
  delete(key: string): Promise<void>;
  putMany(entries: Array<[string, unknown]>): Promise<void>;
  getAllKeys(prefix: string): Promise<string[]>;
}

// ---------------------------------------------------------------------------
// Component interfaces
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

export interface IPageManager {
  // Row operations
  prepareInsert(tableId: string, row: Row): Promise<RowId>;
  prepareUpdate(tableId: string, rowId: RowId, row: Row): Promise<RowId>;
  prepareDelete(tableId: string, rowId: RowId): Promise<void>;
  scanTable(tableId: string): AsyncGenerator<{ rowId: RowId; row: Row }>;
  readRow(tableId: string, rowId: RowId): Promise<Row | null>;

  // Page metadata & management
  getAllPageKeys(tableId: string): Promise<string[]>;
  deleteTableData(tableId: string): Promise<void>;

  // KV operations (used by B-tree index nodes)
  readKey<T>(key: string): Promise<T | null>;
  getAllKeys(prefix: string): Promise<string[]>;
  writeKey(key: string, value: unknown): void;
  deleteKey(key: string): void;

  // Transaction control
  commit(): Promise<void>;
  rollback(): void;
}

/** Subset of IPageManager used by executor / physical operators. */
export type IRowManager = Pick<
  IPageManager,
  "prepareInsert" | "prepareUpdate" | "prepareDelete" | "scanTable" | "readRow"
>;

