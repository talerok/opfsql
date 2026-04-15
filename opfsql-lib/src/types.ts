export type LogicalType =
  | 'INTEGER'
  | 'BIGINT'
  | 'REAL'
  | 'TEXT'
  | 'BLOB'
  | 'BOOLEAN'
  | 'JSON'
  | 'NULL'
  | 'ANY';

export type JsonValue =
  | { [key: string]: JsonValue }
  | JsonValue[]
  | string
  | number
  | boolean
  | null;

export type Value = string | number | boolean | null | JsonValue | Uint8Array;

export interface ColumnDef {
  name: string;
  type: LogicalType;
  nullable: boolean;
  primaryKey: boolean;
  unique: boolean;
  autoIncrement: boolean;
  defaultValue: Value;
}

export interface TableSchema {
  name: string;
  columns: ColumnDef[];
  autoIncrementSeq?: number;
  metaPageNo?: number;
}

export interface IndexDef {
  name: string;
  tableName: string;
  columns: string[];
  unique: boolean;
  metaPageNo?: number;
}

export interface CatalogData {
  tables: TableSchema[];
  indexes: IndexDef[];
}

export type RowId = number;
export type Row = Record<string, Value>;
