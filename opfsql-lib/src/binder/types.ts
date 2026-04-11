import type { LogicalType, TableSchema, ColumnDef, IndexDef } from '../store/types.js';

export type { LogicalType, TableSchema, ColumnDef, IndexDef };

// ============================================================================
// Enums
// ============================================================================

export enum LogicalOperatorType {
  LOGICAL_GET = 'LOGICAL_GET',
  LOGICAL_FILTER = 'LOGICAL_FILTER',
  LOGICAL_PROJECTION = 'LOGICAL_PROJECTION',
  LOGICAL_AGGREGATE_AND_GROUP_BY = 'LOGICAL_AGGREGATE_AND_GROUP_BY',
  LOGICAL_COMPARISON_JOIN = 'LOGICAL_COMPARISON_JOIN',
  LOGICAL_CROSS_PRODUCT = 'LOGICAL_CROSS_PRODUCT',
  LOGICAL_ORDER_BY = 'LOGICAL_ORDER_BY',
  LOGICAL_LIMIT = 'LOGICAL_LIMIT',
  LOGICAL_DISTINCT = 'LOGICAL_DISTINCT',
  LOGICAL_UNION = 'LOGICAL_UNION',
  LOGICAL_INSERT = 'LOGICAL_INSERT',
  LOGICAL_UPDATE = 'LOGICAL_UPDATE',
  LOGICAL_DELETE = 'LOGICAL_DELETE',
  LOGICAL_CREATE_TABLE = 'LOGICAL_CREATE_TABLE',
  LOGICAL_CREATE_INDEX = 'LOGICAL_CREATE_INDEX',
  LOGICAL_ALTER_TABLE = 'LOGICAL_ALTER_TABLE',
  LOGICAL_DROP = 'LOGICAL_DROP',
  LOGICAL_CTE_REF = 'LOGICAL_CTE_REF',
  LOGICAL_MATERIALIZED_CTE = 'LOGICAL_MATERIALIZED_CTE',
  LOGICAL_RECURSIVE_CTE = 'LOGICAL_RECURSIVE_CTE',
}

export enum BoundExpressionClass {
  BOUND_COLUMN_REF = 'BOUND_COLUMN_REF',
  BOUND_CONSTANT = 'BOUND_CONSTANT',
  BOUND_COMPARISON = 'BOUND_COMPARISON',
  BOUND_CONJUNCTION = 'BOUND_CONJUNCTION',
  BOUND_OPERATOR = 'BOUND_OPERATOR',
  BOUND_BETWEEN = 'BOUND_BETWEEN',
  BOUND_FUNCTION = 'BOUND_FUNCTION',
  BOUND_AGGREGATE = 'BOUND_AGGREGATE',
  BOUND_SUBQUERY = 'BOUND_SUBQUERY',
  BOUND_CASE = 'BOUND_CASE',
  BOUND_CAST = 'BOUND_CAST',
}

// ============================================================================
// ColumnBinding
// ============================================================================

export interface ColumnBinding {
  tableIndex: number;
  columnIndex: number;
}

// ============================================================================
// BoundExpression
// ============================================================================

export interface BoundColumnRefExpression {
  expressionClass: BoundExpressionClass.BOUND_COLUMN_REF;
  binding: ColumnBinding;
  tableName: string;
  columnName: string;
  returnType: LogicalType;
}

export interface BoundConstantExpression {
  expressionClass: BoundExpressionClass.BOUND_CONSTANT;
  value: string | number | boolean | null;
  returnType: LogicalType;
}

export type ComparisonType =
  | 'EQUAL'
  | 'NOT_EQUAL'
  | 'LESS'
  | 'GREATER'
  | 'LESS_EQUAL'
  | 'GREATER_EQUAL';

export interface BoundComparisonExpression {
  expressionClass: BoundExpressionClass.BOUND_COMPARISON;
  comparisonType: ComparisonType;
  left: BoundExpression;
  right: BoundExpression;
  returnType: 'BOOLEAN';
}

export interface BoundConjunctionExpression {
  expressionClass: BoundExpressionClass.BOUND_CONJUNCTION;
  conjunctionType: 'AND' | 'OR';
  children: BoundExpression[];
  returnType: 'BOOLEAN';
}

export type OperatorType =
  | 'NOT'
  | 'IS_NULL'
  | 'IS_NOT_NULL'
  | 'NEGATE'
  | 'IN'
  | 'NOT_IN'
  | 'ADD'
  | 'SUBTRACT'
  | 'MULTIPLY'
  | 'DIVIDE'
  | 'MOD';

export interface BoundOperatorExpression {
  expressionClass: BoundExpressionClass.BOUND_OPERATOR;
  operatorType: OperatorType;
  children: BoundExpression[];
  returnType: LogicalType;
}

export interface BoundBetweenExpression {
  expressionClass: BoundExpressionClass.BOUND_BETWEEN;
  input: BoundExpression;
  lower: BoundExpression;
  upper: BoundExpression;
  returnType: 'BOOLEAN';
}

export interface BoundFunctionExpression {
  expressionClass: BoundExpressionClass.BOUND_FUNCTION;
  functionName: string;
  children: BoundExpression[];
  returnType: LogicalType;
}

export type AggregateFunctionName = 'COUNT' | 'SUM' | 'AVG' | 'MIN' | 'MAX';

export interface BoundAggregateExpression {
  expressionClass: BoundExpressionClass.BOUND_AGGREGATE;
  functionName: AggregateFunctionName;
  children: BoundExpression[];
  distinct: boolean;
  isStar: boolean;
  aggregateIndex: number;
  binding?: ColumnBinding;
  returnType: LogicalType;
}

export interface BoundSubqueryExpression {
  expressionClass: BoundExpressionClass.BOUND_SUBQUERY;
  subqueryType: 'SCALAR' | 'EXISTS' | 'NOT_EXISTS' | 'ANY' | 'ALL';
  subplan: LogicalOperator;
  comparisonType?: ComparisonType;
  child?: BoundExpression;
  returnType: LogicalType;
}

export interface BoundCaseExpression {
  expressionClass: BoundExpressionClass.BOUND_CASE;
  caseChecks: Array<{ when: BoundExpression; then: BoundExpression }>;
  elseExpr: BoundExpression | null;
  returnType: LogicalType;
}

export interface BoundCastExpression {
  expressionClass: BoundExpressionClass.BOUND_CAST;
  child: BoundExpression;
  castType: LogicalType;
  returnType: LogicalType;
}

export type BoundExpression =
  | BoundColumnRefExpression
  | BoundConstantExpression
  | BoundComparisonExpression
  | BoundConjunctionExpression
  | BoundOperatorExpression
  | BoundBetweenExpression
  | BoundFunctionExpression
  | BoundAggregateExpression
  | BoundSubqueryExpression
  | BoundCaseExpression
  | BoundCastExpression;

// ============================================================================
// LogicalOperator
// ============================================================================

export interface TableFilter {
  columnIndex: number;
  comparisonType: ComparisonType;
  constant: BoundConstantExpression;
}

export interface JoinCondition {
  left: BoundExpression;
  right: BoundExpression;
  comparisonType: ComparisonType;
}

export interface BoundOrderByNode {
  expression: BoundExpression;
  orderType: 'ASCENDING' | 'DESCENDING';
  nullOrder: 'NULLS_FIRST' | 'NULLS_LAST';
}

export interface IndexSearchPredicate {
  /** Column position in the index's column list (0-based). */
  columnPosition: number;
  comparisonType: Exclude<ComparisonType, 'NOT_EQUAL'>;
  value: string | number | boolean | null;
}

export interface IndexHint {
  indexDef: IndexDef;
  predicates: IndexSearchPredicate[];
  /** Filters NOT covered by the index (need residual filtering in scan). */
  residualFilters: TableFilter[];
  /** Filters covered by the index (can be skipped in scan). */
  coveredFilters: TableFilter[];
}

export interface LogicalGet {
  type: LogicalOperatorType.LOGICAL_GET;
  children: LogicalOperator[];
  expressions: BoundExpression[];
  types: LogicalType[];
  estimatedCardinality: number;
  tableIndex: number;
  tableName: string;
  schema: TableSchema;
  columnIds: number[];
  tableFilters: TableFilter[];
  indexHint?: IndexHint;
  getColumnBindings(): ColumnBinding[];
}

export interface LogicalFilter {
  type: LogicalOperatorType.LOGICAL_FILTER;
  children: [LogicalOperator];
  expressions: BoundExpression[];
  types: LogicalType[];
  estimatedCardinality: number;
  getColumnBindings(): ColumnBinding[];
}

export interface LogicalProjection {
  type: LogicalOperatorType.LOGICAL_PROJECTION;
  tableIndex: number;
  children: [LogicalOperator];
  expressions: BoundExpression[];
  aliases: (string | null)[];
  types: LogicalType[];
  estimatedCardinality: number;
  getColumnBindings(): ColumnBinding[];
}

export interface LogicalAggregate {
  type: LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY;
  groupIndex: number;
  aggregateIndex: number;
  children: [LogicalOperator];
  expressions: BoundAggregateExpression[];
  groups: BoundExpression[];
  havingExpression: BoundExpression | null;
  types: LogicalType[];
  estimatedCardinality: number;
  getColumnBindings(): ColumnBinding[];
}

export interface LogicalComparisonJoin {
  type: LogicalOperatorType.LOGICAL_COMPARISON_JOIN;
  joinType: 'INNER' | 'LEFT' | 'SEMI' | 'ANTI';
  children: [LogicalOperator, LogicalOperator];
  conditions: JoinCondition[];
  expressions: BoundExpression[];
  types: LogicalType[];
  estimatedCardinality: number;
  getColumnBindings(): ColumnBinding[];
}

export interface LogicalCrossProduct {
  type: LogicalOperatorType.LOGICAL_CROSS_PRODUCT;
  children: [LogicalOperator, LogicalOperator];
  expressions: BoundExpression[];
  types: LogicalType[];
  estimatedCardinality: number;
  getColumnBindings(): ColumnBinding[];
}

export interface LogicalOrderBy {
  type: LogicalOperatorType.LOGICAL_ORDER_BY;
  children: [LogicalOperator];
  orders: BoundOrderByNode[];
  expressions: BoundExpression[];
  types: LogicalType[];
  estimatedCardinality: number;
  topN?: number;
  getColumnBindings(): ColumnBinding[];
}

export interface LogicalLimit {
  type: LogicalOperatorType.LOGICAL_LIMIT;
  children: [LogicalOperator];
  limitVal: number | null;
  offsetVal: number;
  expressions: BoundExpression[];
  types: LogicalType[];
  estimatedCardinality: number;
  getColumnBindings(): ColumnBinding[];
}

export interface LogicalDistinct {
  type: LogicalOperatorType.LOGICAL_DISTINCT;
  children: [LogicalOperator];
  expressions: BoundExpression[];
  types: LogicalType[];
  estimatedCardinality: number;
  getColumnBindings(): ColumnBinding[];
}

export interface LogicalUnion {
  type: LogicalOperatorType.LOGICAL_UNION;
  children: [LogicalOperator, LogicalOperator];
  all: boolean;
  expressions: BoundExpression[];
  types: LogicalType[];
  estimatedCardinality: number;
  getColumnBindings(): ColumnBinding[];
}

export interface LogicalInsert {
  type: LogicalOperatorType.LOGICAL_INSERT;
  tableName: string;
  schema: TableSchema;
  columns: number[];
  children: LogicalOperator[];
  expressions: BoundExpression[];
  types: LogicalType[];
  estimatedCardinality: number;
  getColumnBindings(): ColumnBinding[];
}

export interface LogicalUpdate {
  type: LogicalOperatorType.LOGICAL_UPDATE;
  tableName: string;
  schema: TableSchema;
  children: [LogicalOperator];
  updateColumns: number[];
  expressions: BoundExpression[];
  types: LogicalType[];
  estimatedCardinality: number;
  getColumnBindings(): ColumnBinding[];
}

export interface LogicalDelete {
  type: LogicalOperatorType.LOGICAL_DELETE;
  tableName: string;
  schema: TableSchema;
  children: [LogicalOperator];
  expressions: BoundExpression[];
  types: LogicalType[];
  estimatedCardinality: number;
  getColumnBindings(): ColumnBinding[];
}

export interface LogicalCreateTable {
  type: LogicalOperatorType.LOGICAL_CREATE_TABLE;
  schema: TableSchema;
  ifNotExists: boolean;
  children: LogicalOperator[];
  expressions: BoundExpression[];
  types: LogicalType[];
  estimatedCardinality: number;
  getColumnBindings(): ColumnBinding[];
}

export interface LogicalCreateIndex {
  type: LogicalOperatorType.LOGICAL_CREATE_INDEX;
  index: IndexDef;
  ifNotExists: boolean;
  children: LogicalOperator[];
  expressions: BoundExpression[];
  types: LogicalType[];
  estimatedCardinality: number;
  getColumnBindings(): ColumnBinding[];
}

export interface LogicalAlterTable {
  type: LogicalOperatorType.LOGICAL_ALTER_TABLE;
  tableName: string;
  action:
    | { type: 'ADD_COLUMN'; column: ColumnDef }
    | { type: 'DROP_COLUMN'; columnName: string };
  children: LogicalOperator[];
  expressions: BoundExpression[];
  types: LogicalType[];
  estimatedCardinality: number;
  getColumnBindings(): ColumnBinding[];
}

export interface LogicalDrop {
  type: LogicalOperatorType.LOGICAL_DROP;
  dropType: 'TABLE' | 'INDEX';
  name: string;
  ifExists: boolean;
  children: LogicalOperator[];
  expressions: BoundExpression[];
  types: LogicalType[];
  estimatedCardinality: number;
  getColumnBindings(): ColumnBinding[];
}

export interface LogicalCTERef {
  type: LogicalOperatorType.LOGICAL_CTE_REF;
  cteName: string;
  cteIndex: number;
  children: LogicalOperator[];
  expressions: BoundExpression[];
  types: LogicalType[];
  estimatedCardinality: number;
  getColumnBindings(): ColumnBinding[];
}

export interface LogicalMaterializedCTE {
  type: LogicalOperatorType.LOGICAL_MATERIALIZED_CTE;
  cteName: string;
  cteIndex: number;
  children: [LogicalOperator, LogicalOperator];
  expressions: BoundExpression[];
  types: LogicalType[];
  estimatedCardinality: number;
  getColumnBindings(): ColumnBinding[];
}

export interface LogicalRecursiveCTE {
  type: LogicalOperatorType.LOGICAL_RECURSIVE_CTE;
  cteName: string;
  cteIndex: number;
  children: [LogicalOperator, LogicalOperator]; // [anchor, recursive]
  isUnionAll: boolean;
  expressions: BoundExpression[];
  types: LogicalType[];
  estimatedCardinality: number;
  getColumnBindings(): ColumnBinding[];
}

export type LogicalOperator =
  | LogicalGet
  | LogicalFilter
  | LogicalProjection
  | LogicalAggregate
  | LogicalComparisonJoin
  | LogicalCrossProduct
  | LogicalOrderBy
  | LogicalLimit
  | LogicalDistinct
  | LogicalUnion
  | LogicalInsert
  | LogicalUpdate
  | LogicalDelete
  | LogicalCreateTable
  | LogicalCreateIndex
  | LogicalAlterTable
  | LogicalDrop
  | LogicalCTERef
  | LogicalMaterializedCTE
  | LogicalRecursiveCTE;
