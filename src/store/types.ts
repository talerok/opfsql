export type LogicalType =
  | 'INTEGER'
  | 'BIGINT'
  | 'REAL'
  | 'TEXT'
  | 'BLOB'
  | 'BOOLEAN'
  | 'NULL'
  | 'ANY';

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

export interface RowId {
  pageId: number;
  slotId: number;
}

export type Row = Record<string, string | number | boolean | null>;

export interface PageRow {
  slotId: number;
  deleted: boolean;
  data: Row;
}

export interface Page {
  pageId: number;
  tableId: string;
  rows: PageRow[];
}

export interface PageMeta {
  lastPageId: number;
  totalRowCount: number;
  deadRowCount: number;
}

export const PAGE_SIZE = 500;

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
  readPage(tableId: string, pageId: number): Promise<Page | null>;
  getPageMeta(tableId: string): Promise<PageMeta>;
  createEmptyPage(tableId: string, pageId: number): Page;
  getPageKey(tableId: string, pageId: number): string;
  getMetaKey(tableId: string): string;
  getAllPageKeys(tableId: string): Promise<string[]>;
  writePage(tableId: string, page: Page): void;
  writeMeta(tableId: string, meta: PageMeta): void;
  writeKey(key: string, value: unknown): void;
  deleteKey(key: string): void;
  checkpoint(): void;
  restoreCheckpoint(): void;
  commit(): Promise<void>;
  rollback(): void;
}

export interface IRowManager {
  prepareInsert(tableId: string, row: Row): Promise<void>;
  prepareUpdate(tableId: string, rowId: RowId, row: Row): Promise<void>;
  prepareDelete(tableId: string, rowId: RowId): Promise<void>;
  scanTable(tableId: string): AsyncGenerator<{ rowId: RowId; row: Row }>;
}

export interface IVacuum {
  shouldVacuum(tableId: string): Promise<boolean>;
  vacuumTable(tableId: string): Promise<void>;
  vacuumIfNeeded(tableId: string): void;
}
