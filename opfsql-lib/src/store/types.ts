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

/** Logical row identifier — auto-incrementing number managed by TableBTree. */
export type RowId = number;

export type Row = Record<string, string | number | boolean | null>;

// ---------------------------------------------------------------------------
// Storage backend interface (OPFS / IndexedDB / memory)
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
// KV store with WAL + cache (implemented by PageManager)
// ---------------------------------------------------------------------------

export interface IKVStore {
  readKey<T>(key: string): Promise<T | null>;
  getAllKeys(prefix: string): Promise<string[]>;
  writeKey(key: string, value: unknown): void;
  deleteKey(key: string): void;
  commit(): Promise<void>;
  rollback(): void;
}

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
// Row manager interface (implemented by TableManager)
// ---------------------------------------------------------------------------

export interface IRowManager {
  prepareInsert(tableId: string, row: Row): Promise<RowId>;
  prepareUpdate(tableId: string, rowId: RowId, row: Row): Promise<RowId>;
  prepareDelete(tableId: string, rowId: RowId): Promise<void>;
  scanTable(tableId: string): AsyncGenerator<{ rowId: RowId; row: Row }>;
  readRow(tableId: string, rowId: RowId): Promise<Row | null>;
  deleteTableData(tableId: string): Promise<void>;
}
