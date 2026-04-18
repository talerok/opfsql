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

export type JsonPathSegment =
  | { type: 'field'; name: string }
  | { type: 'index'; value: number };

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

// ---------------------------------------------------------------------------
// Index expression — catalog-level AST for computed index keys.
// References columns by name (not binding index) for schema stability.
// ---------------------------------------------------------------------------

export type IndexOperatorType =
  | 'NOT' | 'IS_NULL' | 'IS_NOT_NULL' | 'NEGATE'
  | 'IN' | 'NOT_IN'
  | 'ADD' | 'SUBTRACT' | 'MULTIPLY' | 'DIVIDE' | 'MOD' | 'CONCAT';

export interface IndexCaseCheck {
  when: IndexExpression;
  then: IndexExpression;
}

export type IndexComparisonType = 'EQUAL' | 'NOT_EQUAL' | 'LESS' | 'GREATER' | 'LESS_EQUAL' | 'GREATER_EQUAL';

export type IndexExpression =
  | { type: 'column'; name: string; returnType: LogicalType }
  | { type: 'json_access'; column: string; path: JsonPathSegment[]; returnType: LogicalType }
  | { type: 'function'; name: string; args: IndexExpression[]; returnType: LogicalType }
  | { type: 'cast'; child: IndexExpression; castType: LogicalType; returnType: LogicalType }
  | { type: 'operator'; operatorType: IndexOperatorType; args: IndexExpression[]; returnType: LogicalType }
  | { type: 'comparison'; comparisonType: IndexComparisonType; left: IndexExpression; right: IndexExpression; returnType: LogicalType }
  | { type: 'case'; checks: IndexCaseCheck[]; elseExpr: IndexExpression | null; returnType: LogicalType }
  | { type: 'constant'; value: string | number | boolean | null; returnType: LogicalType };

export interface IndexDef {
  name: string;
  tableName: string;
  expressions: IndexExpression[];
  unique: boolean;
  metaPageNo?: number;
}

export interface CatalogData {
  tables: TableSchema[];
  indexes: IndexDef[];
}

export type RowId = number;
export type Row = Record<string, Value>;
