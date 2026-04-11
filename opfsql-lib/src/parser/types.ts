// ============================================================================
// Token types
// ============================================================================

export enum TokenType {
  // Literals
  INTEGER_LITERAL,
  FLOAT_LITERAL,
  STRING_LITERAL,

  // Identifiers
  IDENTIFIER,
  QUOTED_IDENTIFIER,

  // Operators
  EQUALS,
  NOT_EQUALS,
  LESS_THAN,
  LESS_THAN_EQUAL,
  GREATER_THAN,
  GREATER_THAN_EQUAL,
  PLUS,
  MINUS,
  STAR,
  SLASH,
  PERCENT,

  // Punctuation
  LEFT_PAREN,
  RIGHT_PAREN,
  COMMA,
  SEMICOLON,
  DOT,

  // Keywords
  SELECT,
  FROM,
  WHERE,
  JOIN,
  LEFT,
  RIGHT,
  INNER,
  OUTER,
  CROSS,
  ON,
  USING,
  AS,
  DISTINCT,
  ALL,
  GROUP,
  BY,
  HAVING,
  ORDER,
  ASC,
  DESC,
  NULLS,
  FIRST,
  LAST,
  LIMIT,
  OFFSET,
  UNION,
  INSERT,
  INTO,
  VALUES,
  UPDATE,
  SET,
  DELETE,
  CREATE,
  TABLE,
  INDEX,
  UNIQUE,
  IF,
  NOT,
  EXISTS,
  ALTER,
  ADD,
  DROP,
  COLUMN,
  BEGIN,
  COMMIT,
  ROLLBACK,
  TRANSACTION,
  WITH,
  AND,
  OR,
  IN,
  BETWEEN,
  LIKE,
  IS,
  NULL_KW,
  PRIMARY,
  KEY,
  FOREIGN,
  REFERENCES,
  DEFAULT,
  CASE,
  WHEN,
  THEN,
  ELSE,
  END,
  CAST,
  TRUE_KW,
  FALSE_KW,

  // Type keywords
  INTEGER_KW,
  INT_KW,
  BIGINT_KW,
  SMALLINT_KW,
  REAL_KW,
  FLOAT_KW,
  DOUBLE_KW,
  TEXT_KW,
  VARCHAR_KW,
  CHAR_KW,
  BLOB_KW,
  BOOLEAN_KW,
  BOOL_KW,

  EOF,
}

export interface Token {
  type: TokenType;
  value: string;
  line: number;
  column: number;
}

// ============================================================================
// Logical types (DuckDB-style)
// ============================================================================

export enum LogicalTypeId {
  INTEGER = 'INTEGER',
  BIGINT = 'BIGINT',
  SMALLINT = 'SMALLINT',
  FLOAT = 'FLOAT',
  DOUBLE = 'DOUBLE',
  VARCHAR = 'VARCHAR',
  BLOB = 'BLOB',
  BOOLEAN = 'BOOLEAN',
}

export interface LogicalType {
  id: LogicalTypeId;
}

// ============================================================================
// Values
// ============================================================================

export interface Value {
  type: LogicalType;
  is_null: boolean;
  value: string | number | boolean | null;
}

// ============================================================================
// Expression types (DuckDB-style enums)
// ============================================================================

export enum ExpressionClass {
  COLUMN_REF = 'COLUMN_REF',
  CONSTANT = 'CONSTANT',
  COMPARISON = 'COMPARISON',
  CONJUNCTION = 'CONJUNCTION',
  OPERATOR = 'OPERATOR',
  BETWEEN = 'BETWEEN',
  FUNCTION = 'FUNCTION',
  SUBQUERY = 'SUBQUERY',
  CASE = 'CASE',
  CAST = 'CAST',
  STAR = 'STAR',
}

export enum ExpressionType {
  // Comparison
  COMPARE_EQUAL = 'COMPARE_EQUAL',
  COMPARE_NOTEQUAL = 'COMPARE_NOTEQUAL',
  COMPARE_LESSTHAN = 'COMPARE_LESSTHAN',
  COMPARE_LESSTHANOREQUALTO = 'COMPARE_LESSTHANOREQUALTO',
  COMPARE_GREATERTHAN = 'COMPARE_GREATERTHAN',
  COMPARE_GREATERTHANOREQUALTO = 'COMPARE_GREATERTHANOREQUALTO',

  // Conjunction
  CONJUNCTION_AND = 'CONJUNCTION_AND',
  CONJUNCTION_OR = 'CONJUNCTION_OR',

  // Operators
  OPERATOR_NOT = 'OPERATOR_NOT',
  OPERATOR_IS_NULL = 'OPERATOR_IS_NULL',
  OPERATOR_IS_NOT_NULL = 'OPERATOR_IS_NOT_NULL',
  OPERATOR_IN = 'OPERATOR_IN',
  OPERATOR_NOT_IN = 'OPERATOR_NOT_IN',

  // Like
  COMPARE_LIKE = 'COMPARE_LIKE',
  COMPARE_NOT_LIKE = 'COMPARE_NOT_LIKE',

  // Arithmetic
  OPERATOR_ADD = 'OPERATOR_ADD',
  OPERATOR_SUBTRACT = 'OPERATOR_SUBTRACT',
  OPERATOR_MULTIPLY = 'OPERATOR_MULTIPLY',
  OPERATOR_DIVIDE = 'OPERATOR_DIVIDE',
  OPERATOR_MOD = 'OPERATOR_MOD',
  OPERATOR_NEGATE = 'OPERATOR_NEGATE',
}

// ============================================================================
// Parsed expressions (DuckDB-style)
// ============================================================================

export interface ColumnRefExpression {
  expression_class: ExpressionClass.COLUMN_REF;
  alias: string | null;
  column_names: string[];
}

export interface ConstantExpression {
  expression_class: ExpressionClass.CONSTANT;
  alias: string | null;
  value: Value;
}

export interface ComparisonExpression {
  expression_class: ExpressionClass.COMPARISON;
  alias: string | null;
  type: ExpressionType;
  left: ParsedExpression;
  right: ParsedExpression;
}

export interface ConjunctionExpression {
  expression_class: ExpressionClass.CONJUNCTION;
  alias: string | null;
  type: ExpressionType;
  children: ParsedExpression[];
}

export interface OperatorExpression {
  expression_class: ExpressionClass.OPERATOR;
  alias: string | null;
  type: ExpressionType;
  children: ParsedExpression[];
}

export interface BetweenExpression {
  expression_class: ExpressionClass.BETWEEN;
  alias: string | null;
  input: ParsedExpression;
  lower: ParsedExpression;
  upper: ParsedExpression;
}

export interface FunctionExpression {
  expression_class: ExpressionClass.FUNCTION;
  alias: string | null;
  function_name: string;
  children: ParsedExpression[];
  distinct: boolean;
  is_star: boolean;
}

export interface SubqueryExpression {
  expression_class: ExpressionClass.SUBQUERY;
  alias: string | null;
  subquery_type: 'SCALAR' | 'EXISTS' | 'NOT_EXISTS' | 'ANY' | 'ALL';
  subquery: SelectStatement;
  child: ParsedExpression | null;
  comparison_type?: ExpressionType;
}

export interface CaseExpression {
  expression_class: ExpressionClass.CASE;
  alias: string | null;
  case_checks: Array<{ when_expr: ParsedExpression; then_expr: ParsedExpression }>;
  else_expr: ParsedExpression | null;
}

export interface CastExpression {
  expression_class: ExpressionClass.CAST;
  alias: string | null;
  child: ParsedExpression;
  cast_type: LogicalType;
}

export interface StarExpression {
  expression_class: ExpressionClass.STAR;
  alias: string | null;
  table_name: string | null;
}

export type ParsedExpression =
  | ColumnRefExpression
  | ConstantExpression
  | ComparisonExpression
  | ConjunctionExpression
  | OperatorExpression
  | BetweenExpression
  | FunctionExpression
  | SubqueryExpression
  | CaseExpression
  | CastExpression
  | StarExpression;

// ============================================================================
// Table references (DuckDB-style)
// ============================================================================

export enum TableRefType {
  BASE_TABLE = 'BASE_TABLE',
  JOIN = 'JOIN',
  SUBQUERY = 'SUBQUERY',
}

export enum JoinType {
  INNER = 'INNER',
  LEFT = 'LEFT',
  RIGHT = 'RIGHT',
  CROSS = 'CROSS',
}

export interface BaseTableRef {
  type: TableRefType.BASE_TABLE;
  table_name: string;
  alias: string | null;
  schema_name: string | null;
}

export interface JoinRef {
  type: TableRefType.JOIN;
  left: TableRef;
  right: TableRef;
  condition: ParsedExpression | null;
  join_type: JoinType;
  using_columns: string[];
}

export interface SubqueryRef {
  type: TableRefType.SUBQUERY;
  subquery: SelectStatement;
  alias: string | null;
  column_name_alias: string[];
}

export type TableRef = BaseTableRef | JoinRef | SubqueryRef;

// ============================================================================
// Result modifiers (DuckDB-style)
// ============================================================================

export enum ResultModifierType {
  ORDER_MODIFIER = 'ORDER_MODIFIER',
  LIMIT_MODIFIER = 'LIMIT_MODIFIER',
  DISTINCT_MODIFIER = 'DISTINCT_MODIFIER',
}

export enum OrderType {
  ASCENDING = 'ASCENDING',
  DESCENDING = 'DESCENDING',
}

export enum OrderByNullType {
  NULLS_FIRST = 'NULLS_FIRST',
  NULLS_LAST = 'NULLS_LAST',
}

export interface OrderByNode {
  type: OrderType;
  null_order: OrderByNullType;
  expression: ParsedExpression;
}

export interface OrderModifier {
  type: ResultModifierType.ORDER_MODIFIER;
  orders: OrderByNode[];
}

export interface LimitModifier {
  type: ResultModifierType.LIMIT_MODIFIER;
  limit: ParsedExpression | null;
  offset: ParsedExpression | null;
}

export interface DistinctModifier {
  type: ResultModifierType.DISTINCT_MODIFIER;
  distinct_on_targets: ParsedExpression[];
}

export type ResultModifier = OrderModifier | LimitModifier | DistinctModifier;

// ============================================================================
// Query nodes (DuckDB-style)
// ============================================================================

export interface GroupByNode {
  group_expressions: ParsedExpression[];
}

export interface CTENode {
  query: SelectStatement;
  aliases: string[];
}

export interface CTEMap {
  map: Record<string, CTENode>;
  recursive: boolean;
}

export interface SelectNode {
  type: 'SELECT_NODE';
  select_list: ParsedExpression[];
  from_table: TableRef | null;
  where_clause: ParsedExpression | null;
  groups: GroupByNode;
  having: ParsedExpression | null;
  modifiers: ResultModifier[];
  cte_map: CTEMap;
}

// ============================================================================
// Statements
// ============================================================================

export enum StatementType {
  SELECT_STATEMENT = 'SELECT_STATEMENT',
  INSERT_STATEMENT = 'INSERT_STATEMENT',
  UPDATE_STATEMENT = 'UPDATE_STATEMENT',
  DELETE_STATEMENT = 'DELETE_STATEMENT',
  CREATE_TABLE_STATEMENT = 'CREATE_TABLE_STATEMENT',
  CREATE_INDEX_STATEMENT = 'CREATE_INDEX_STATEMENT',
  ALTER_TABLE_STATEMENT = 'ALTER_TABLE_STATEMENT',
  DROP_STATEMENT = 'DROP_STATEMENT',
  TRANSACTION_STATEMENT = 'TRANSACTION_STATEMENT',
}

export interface SelectStatement {
  type: StatementType.SELECT_STATEMENT;
  node: SelectNode | SetOperationNode;
}

// SetOperation for UNION
export enum SetOperationType {
  UNION = 'UNION',
  UNION_ALL = 'UNION_ALL',
}

export interface SetOperationNode {
  type: 'SET_OPERATION_NODE';
  set_op_type: SetOperationType;
  left: SelectNode | SetOperationNode;
  right: SelectNode;
  modifiers: ResultModifier[];
  cte_map: CTEMap;
}

export interface SetOperationStatement {
  type: StatementType.SELECT_STATEMENT;
  node: SetOperationNode;
}

// Column definition for CREATE TABLE
export interface ColumnDefinition {
  name: string;
  type: LogicalType;
  is_primary_key: boolean;
  is_not_null: boolean;
  is_unique: boolean;
  default_value: ParsedExpression | null;
}

export interface ForeignKeyConstraint {
  columns: string[];
  ref_table: string;
  ref_columns: string[];
}

export interface InsertStatement {
  type: StatementType.INSERT_STATEMENT;
  table: string;
  columns: string[];
  values: ParsedExpression[][];
  select_statement: SelectStatement | null;
}

export interface UpdateSetClause {
  column: string;
  value: ParsedExpression;
}

export interface UpdateStatement {
  type: StatementType.UPDATE_STATEMENT;
  table: string;
  set_clauses: UpdateSetClause[];
  where_clause: ParsedExpression | null;
}

export interface DeleteStatement {
  type: StatementType.DELETE_STATEMENT;
  table: string;
  where_clause: ParsedExpression | null;
}

export interface CreateTableStatement {
  type: StatementType.CREATE_TABLE_STATEMENT;
  table: string;
  if_not_exists: boolean;
  columns: ColumnDefinition[];
  primary_key: string[];
  foreign_keys: ForeignKeyConstraint[];
}

export interface CreateIndexStatement {
  type: StatementType.CREATE_INDEX_STATEMENT;
  index_name: string;
  table_name: string;
  columns: string[];
  is_unique: boolean;
  if_not_exists: boolean;
}

export enum AlterType {
  ADD_COLUMN = 'ADD_COLUMN',
  DROP_COLUMN = 'DROP_COLUMN',
}

export interface AlterTableStatement {
  type: StatementType.ALTER_TABLE_STATEMENT;
  table: string;
  alter_type: AlterType;
  column_def: ColumnDefinition | null;
  column_name: string | null;
}

export enum DropType {
  TABLE = 'TABLE',
  INDEX = 'INDEX',
}

export interface DropStatement {
  type: StatementType.DROP_STATEMENT;
  drop_type: DropType;
  name: string;
  if_exists: boolean;
}

export enum TransactionType {
  BEGIN = 'BEGIN',
  COMMIT = 'COMMIT',
  ROLLBACK = 'ROLLBACK',
}

export interface TransactionStatement {
  type: StatementType.TRANSACTION_STATEMENT;
  transaction_type: TransactionType;
}

export type Statement =
  | SelectStatement
  | SetOperationStatement
  | InsertStatement
  | UpdateStatement
  | DeleteStatement
  | CreateTableStatement
  | CreateIndexStatement
  | AlterTableStatement
  | DropStatement
  | TransactionStatement;

// ============================================================================
// Parse error
// ============================================================================

export class ParseError extends Error {
  constructor(
    message: string,
    public line: number,
    public column: number,
    public token: Token
  ) {
    super(`Parse error at line ${line}, column ${column}: ${message}`);
    this.name = 'ParseError';
  }
}
