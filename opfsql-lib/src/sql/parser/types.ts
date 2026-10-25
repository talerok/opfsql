// ---------------------------------------------------------------------------
// The syntax tree the parser builds. It records what the statement says, not
// what it means: names are unresolved, types are the declared text, and
// defaults the SQL leaves open (NULLS FIRST or LAST) stay open. Every node is
// readonly; a pass that changes a tree builds a new one.
// ---------------------------------------------------------------------------

// ============================================================================
// Expressions
// ============================================================================

export enum ExpressionClass {
  COLUMN_REF = "COLUMN_REF",
  CONSTANT = "CONSTANT",
  PARAMETER = "PARAMETER",
  COMPARISON = "COMPARISON",
  CONJUNCTION = "CONJUNCTION",
  OPERATOR = "OPERATOR",
  BETWEEN = "BETWEEN",
  FUNCTION = "FUNCTION",
  SUBQUERY = "SUBQUERY",
  CASE = "CASE",
  CAST = "CAST",
  STAR = "STAR",
}

export enum ExpressionType {
  // Comparison
  COMPARE_EQUAL = "COMPARE_EQUAL",
  COMPARE_NOTEQUAL = "COMPARE_NOTEQUAL",
  COMPARE_LESSTHAN = "COMPARE_LESSTHAN",
  COMPARE_LESSTHANOREQUALTO = "COMPARE_LESSTHANOREQUALTO",
  COMPARE_GREATERTHAN = "COMPARE_GREATERTHAN",
  COMPARE_GREATERTHANOREQUALTO = "COMPARE_GREATERTHANOREQUALTO",

  // Conjunction
  CONJUNCTION_AND = "CONJUNCTION_AND",
  CONJUNCTION_OR = "CONJUNCTION_OR",

  // Operators
  OPERATOR_NOT = "OPERATOR_NOT",
  OPERATOR_IS_NULL = "OPERATOR_IS_NULL",
  OPERATOR_IS_NOT_NULL = "OPERATOR_IS_NOT_NULL",
  OPERATOR_IN = "OPERATOR_IN",
  OPERATOR_NOT_IN = "OPERATOR_NOT_IN",

  // Like
  COMPARE_LIKE = "COMPARE_LIKE",
  COMPARE_NOT_LIKE = "COMPARE_NOT_LIKE",

  // Arithmetic
  OPERATOR_ADD = "OPERATOR_ADD",
  OPERATOR_SUBTRACT = "OPERATOR_SUBTRACT",
  OPERATOR_MULTIPLY = "OPERATOR_MULTIPLY",
  OPERATOR_DIVIDE = "OPERATOR_DIVIDE",
  OPERATOR_MOD = "OPERATOR_MOD",
  OPERATOR_NEGATE = "OPERATOR_NEGATE",

  // String
  OPERATOR_CONCAT = "OPERATOR_CONCAT",
}

/** A step into a JSON value after a column: `.field` or `[index]`. */
export type JsonPathSegment =
  | { readonly type: "field"; readonly name: string }
  | { readonly type: "index"; readonly value: number };

/**
 * `a`, `t.a`, or a JSON path `t.a.b[0].c`. The dotted names come first, in
 * `columnNames`; whether the first is a table is the binder's call. What
 * follows the first bracket is `path`.
 */
export interface ColumnReferenceExpression {
  readonly expressionClass: ExpressionClass.COLUMN_REF;
  readonly alias: string | null;
  readonly columnNames: readonly string[];
  readonly path?: readonly JsonPathSegment[];
}

/** The type a literal has as written: `1` is INTEGER, `1.0` and `1e0` REAL. */
export type LiteralType =
  "INTEGER" | "REAL" | "TEXT" | "BLOB" | "BOOLEAN" | "NULL";

export type LiteralValue = number | string | Uint8Array | boolean | null;

/** A literal. A sign written before a number is part of it: `-1`. */
export interface ConstantExpression {
  readonly expressionClass: ExpressionClass.CONSTANT;
  readonly alias: string | null;
  readonly type: LiteralType;
  readonly value: LiteralValue;
}

/**
 * `$1` or `?`, as a 0-based index: `$1` is 0, and each `?` is one more than
 * the highest number used before it in the statement.
 */
export interface ParameterExpression {
  readonly expressionClass: ExpressionClass.PARAMETER;
  readonly alias: string | null;
  readonly index: number;
}

export interface ComparisonExpression {
  readonly expressionClass: ExpressionClass.COMPARISON;
  readonly alias: string | null;
  readonly type: ExpressionType;
  readonly left: ParsedExpression;
  readonly right: ParsedExpression;
}

/** A chain of ANDs or of ORs, flattened: `a AND b AND c` has three children. */
export interface ConjunctionExpression {
  readonly expressionClass: ExpressionClass.CONJUNCTION;
  readonly alias: string | null;
  readonly type: ExpressionType;
  readonly children: readonly ParsedExpression[];
}

/** Arithmetic, NOT, IS [NOT] NULL, and IN with a list (the left side first). */
export interface OperatorExpression {
  readonly expressionClass: ExpressionClass.OPERATOR;
  readonly alias: string | null;
  readonly type: ExpressionType;
  readonly children: readonly ParsedExpression[];
}

export interface BetweenExpression {
  readonly expressionClass: ExpressionClass.BETWEEN;
  readonly alias: string | null;
  readonly input: ParsedExpression;
  readonly lower: ParsedExpression;
  readonly upper: ParsedExpression;
}

/** A call; `count(*)` has `isStar` and no children. Names are lower case. */
export interface FunctionExpression {
  readonly expressionClass: ExpressionClass.FUNCTION;
  readonly alias: string | null;
  readonly functionName: string;
  readonly children: readonly ParsedExpression[];
  readonly distinct: boolean;
  readonly isStar: boolean;
}

/**
 * A subquery as a value (SCALAR), a test (EXISTS, NOT_EXISTS), or a
 * quantified comparison `child <comparisonType> ANY|ALL (subquery)`;
 * `x IN (subquery)` is `x = ANY (subquery)`.
 */
export interface SubqueryExpression {
  readonly expressionClass: ExpressionClass.SUBQUERY;
  readonly alias: string | null;
  readonly subqueryType: "SCALAR" | "EXISTS" | "NOT_EXISTS" | "ANY" | "ALL";
  readonly subquery: SelectStatement;
  readonly child: ParsedExpression | null;
  readonly comparisonType?: ExpressionType;
}

export interface CaseCheck {
  readonly whenExpression: ParsedExpression;
  readonly thenExpression: ParsedExpression;
}

/**
 * `CASE [operand] WHEN … THEN … [ELSE …] END`. With an operand, each
 * `whenExpression` is a value compared with it; without, a condition.
 */
export interface CaseExpression {
  readonly expressionClass: ExpressionClass.CASE;
  readonly alias: string | null;
  readonly operand: ParsedExpression | null;
  readonly caseChecks: readonly CaseCheck[];
  readonly elseExpression: ParsedExpression | null;
}

export interface CastExpression {
  readonly expressionClass: ExpressionClass.CAST;
  readonly alias: string | null;
  readonly child: ParsedExpression;
  /** The type as written, such as `varchar(10)`. */
  readonly castType: string;
}

/** `*` or `t.*`; the parser builds it only as an item of a select list. */
export interface StarExpression {
  readonly expressionClass: ExpressionClass.STAR;
  readonly alias: string | null;
  readonly tableName: string | null;
}

export type ParsedExpression =
  | ColumnReferenceExpression
  | ConstantExpression
  | ParameterExpression
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
// Table references
// ============================================================================

export enum TableReferenceType {
  BASE_TABLE = "BASE_TABLE",
  JOIN = "JOIN",
  SUBQUERY = "SUBQUERY",
}

export enum JoinType {
  INNER = "INNER",
  LEFT = "LEFT",
  RIGHT = "RIGHT",
  CROSS = "CROSS",
}

export interface BaseTableReference {
  readonly type: TableReferenceType.BASE_TABLE;
  readonly tableName: string;
  readonly alias: string | null;
}

/** A join; `FROM a, b` is a CROSS join. */
export interface JoinReference {
  readonly type: TableReferenceType.JOIN;
  readonly left: TableReference;
  readonly right: TableReference;
  readonly condition: ParsedExpression | null;
  readonly joinType: JoinType;
  readonly usingColumns: readonly string[];
}

export interface SubqueryReference {
  readonly type: TableReferenceType.SUBQUERY;
  readonly subquery: SelectStatement;
  readonly alias: string | null;
}

export type TableReference =
  BaseTableReference | JoinReference | SubqueryReference;

// ============================================================================
// Result modifiers
// ============================================================================

export enum ResultModifierType {
  ORDER_MODIFIER = "ORDER_MODIFIER",
  LIMIT_MODIFIER = "LIMIT_MODIFIER",
  DISTINCT_MODIFIER = "DISTINCT_MODIFIER",
}

export enum OrderType {
  ASCENDING = "ASCENDING",
  DESCENDING = "DESCENDING",
}

export enum OrderByNullType {
  NULLS_FIRST = "NULLS_FIRST",
  NULLS_LAST = "NULLS_LAST",
}

export interface OrderByNode {
  readonly type: OrderType;
  /** NULLS FIRST or LAST as written; null when the query does not say. */
  readonly nullOrder: OrderByNullType | null;
  readonly expression: ParsedExpression;
}

export interface OrderModifier {
  readonly type: ResultModifierType.ORDER_MODIFIER;
  readonly orders: readonly OrderByNode[];
}

/** LIMIT and OFFSET; `LIMIT a, b` is `LIMIT b OFFSET a`. */
export interface LimitModifier {
  readonly type: ResultModifierType.LIMIT_MODIFIER;
  readonly limit: ParsedExpression | null;
  readonly offset: ParsedExpression | null;
}

export interface DistinctModifier {
  readonly type: ResultModifierType.DISTINCT_MODIFIER;
}

/** In the order they apply: DISTINCT, then ORDER BY, then LIMIT. */
export type ResultModifier = OrderModifier | LimitModifier | DistinctModifier;

// ============================================================================
// Queries
// ============================================================================

export interface GroupByNode {
  readonly groupExpressions: readonly ParsedExpression[];
}

export interface CTENode {
  readonly name: string;
  readonly query: SelectStatement;
  /** Column names given after the CTE's name, if any. */
  readonly aliases: readonly string[];
}

/** The WITH clause of a query, in the order written. */
export interface CTEMap {
  readonly ctes: readonly CTENode[];
  readonly recursive: boolean;
}

export interface SelectNode {
  readonly type: "SELECT_NODE";
  readonly selectList: readonly ParsedExpression[];
  readonly fromTable: TableReference | null;
  readonly whereClause: ParsedExpression | null;
  readonly groups: GroupByNode;
  readonly having: ParsedExpression | null;
  readonly modifiers: readonly ResultModifier[];
  readonly cteMap: CTEMap;
}

export enum SetOperationType {
  UNION = "UNION",
  UNION_ALL = "UNION_ALL",
  INTERSECT = "INTERSECT",
  EXCEPT = "EXCEPT",
}

/**
 * A compound query. Its operators bind left to right with equal precedence,
 * as in SQLite; WITH, ORDER BY and LIMIT belong to the outermost node.
 */
export interface SetOperationNode {
  readonly type: "SET_OPERATION_NODE";
  readonly setOperationType: SetOperationType;
  readonly left: SelectNode | SetOperationNode;
  readonly right: SelectNode;
  readonly modifiers: readonly ResultModifier[];
  readonly cteMap: CTEMap;
}

// ============================================================================
// Statements
// ============================================================================

export enum StatementType {
  SELECT_STATEMENT = "SELECT_STATEMENT",
  INSERT_STATEMENT = "INSERT_STATEMENT",
  UPDATE_STATEMENT = "UPDATE_STATEMENT",
  DELETE_STATEMENT = "DELETE_STATEMENT",
  CREATE_TABLE_STATEMENT = "CREATE_TABLE_STATEMENT",
  CREATE_INDEX_STATEMENT = "CREATE_INDEX_STATEMENT",
  ALTER_TABLE_STATEMENT = "ALTER_TABLE_STATEMENT",
  DROP_STATEMENT = "DROP_STATEMENT",
  TRANSACTION_STATEMENT = "TRANSACTION_STATEMENT",
  EXPLAIN_STATEMENT = "EXPLAIN_STATEMENT",
}

export interface SelectStatement {
  readonly type: StatementType.SELECT_STATEMENT;
  readonly node: SelectNode | SetOperationNode;
}

export interface ColumnDefinition {
  readonly name: string;
  /** The declared type as written (`varchar(255)`), or null if none. */
  readonly type: string | null;
  readonly isPrimaryKey: boolean;
  readonly isNotNull: boolean;
  readonly isUnique: boolean;
  readonly isAutoincrement: boolean;
  readonly defaultValue: ParsedExpression | null;
}

export interface UpdateSetClause {
  readonly column: string;
  readonly value: ParsedExpression;
}

export interface OnConflictUpdate {
  readonly type: "UPDATE";
  readonly setClauses: readonly UpdateSetClause[];
  readonly whereClause: ParsedExpression | null;
}

export interface OnConflictClause {
  readonly conflictTarget: readonly string[] | null;
  readonly action: "NOTHING" | OnConflictUpdate;
}

export interface InsertStatement {
  readonly type: StatementType.INSERT_STATEMENT;
  readonly table: string;
  readonly columns: readonly string[];
  readonly values: readonly (readonly ParsedExpression[])[];
  readonly selectStatement: SelectStatement | null;
  readonly onConflict: OnConflictClause | null;
}

export interface UpdateStatement {
  readonly type: StatementType.UPDATE_STATEMENT;
  readonly table: string;
  readonly setClauses: readonly UpdateSetClause[];
  readonly whereClause: ParsedExpression | null;
}

export interface DeleteStatement {
  readonly type: StatementType.DELETE_STATEMENT;
  readonly table: string;
  readonly whereClause: ParsedExpression | null;
}

export interface CreateTableStatement {
  readonly type: StatementType.CREATE_TABLE_STATEMENT;
  readonly table: string;
  readonly ifNotExists: boolean;
  readonly columns: readonly ColumnDefinition[];
  /** Columns of a table-level PRIMARY KEY (…) constraint. */
  readonly primaryKey: readonly string[];
}

export interface CreateIndexStatement {
  readonly type: StatementType.CREATE_INDEX_STATEMENT;
  readonly indexName: string;
  readonly tableName: string;
  readonly expressions: readonly ParsedExpression[];
  readonly isUnique: boolean;
  readonly ifNotExists: boolean;
}

export enum AlterType {
  ADD_COLUMN = "ADD_COLUMN",
  DROP_COLUMN = "DROP_COLUMN",
}

export interface AlterTableStatement {
  readonly type: StatementType.ALTER_TABLE_STATEMENT;
  readonly table: string;
  readonly alterType: AlterType;
  readonly columnDefinition: ColumnDefinition | null;
  readonly columnName: string | null;
}

export enum DropType {
  TABLE = "TABLE",
  INDEX = "INDEX",
}

export interface DropStatement {
  readonly type: StatementType.DROP_STATEMENT;
  readonly dropType: DropType;
  readonly name: string;
  readonly ifExists: boolean;
}

export enum TransactionType {
  BEGIN = "BEGIN",
  COMMIT = "COMMIT",
  ROLLBACK = "ROLLBACK",
}

export interface TransactionStatement {
  readonly type: StatementType.TRANSACTION_STATEMENT;
  readonly transactionType: TransactionType;
}

export interface ExplainStatement {
  readonly type: StatementType.EXPLAIN_STATEMENT;
  readonly statement: Statement;
}

export type Statement =
  | SelectStatement
  | InsertStatement
  | UpdateStatement
  | DeleteStatement
  | CreateTableStatement
  | CreateIndexStatement
  | AlterTableStatement
  | DropStatement
  | TransactionStatement
  | ExplainStatement;
