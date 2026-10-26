// ---------------------------------------------------------------------------
// The syntax tree the parser builds. It records what the statement says, not
// what it means: names are unresolved, types are the declared text, and
// defaults the SQL leaves open (NULLS FIRST or LAST) stay open. Every node is
// readonly; a pass that changes a tree builds a new one. A node that can be
// in several states is a union, one member per state.
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
  COMPARE_LIKE = "COMPARE_LIKE",

  // Conjunction
  CONJUNCTION_AND = "CONJUNCTION_AND",
  CONJUNCTION_OR = "CONJUNCTION_OR",

  // Operators
  OPERATOR_NOT = "OPERATOR_NOT",
  OPERATOR_IS_NULL = "OPERATOR_IS_NULL",
  OPERATOR_IS_NOT_NULL = "OPERATOR_IS_NOT_NULL",
  OPERATOR_IN = "OPERATOR_IN",
  OPERATOR_NOT_IN = "OPERATOR_NOT_IN",
  OPERATOR_ADD = "OPERATOR_ADD",
  OPERATOR_SUBTRACT = "OPERATOR_SUBTRACT",
  OPERATOR_MULTIPLY = "OPERATOR_MULTIPLY",
  OPERATOR_DIVIDE = "OPERATOR_DIVIDE",
  OPERATOR_MOD = "OPERATOR_MOD",
  OPERATOR_NEGATE = "OPERATOR_NEGATE",
  OPERATOR_CONCAT = "OPERATOR_CONCAT",
}

/** `=`, `<>`, `<`, `<=`, `>`, `>=`: the comparisons ANY and ALL take. */
export type ComparisonType =
  | ExpressionType.COMPARE_EQUAL
  | ExpressionType.COMPARE_NOTEQUAL
  | ExpressionType.COMPARE_LESSTHAN
  | ExpressionType.COMPARE_LESSTHANOREQUALTO
  | ExpressionType.COMPARE_GREATERTHAN
  | ExpressionType.COMPARE_GREATERTHANOREQUALTO;

export type ConjunctionType =
  ExpressionType.CONJUNCTION_AND | ExpressionType.CONJUNCTION_OR;

export type OperatorType =
  | ExpressionType.OPERATOR_NOT
  | ExpressionType.OPERATOR_IS_NULL
  | ExpressionType.OPERATOR_IS_NOT_NULL
  | ExpressionType.OPERATOR_IN
  | ExpressionType.OPERATOR_NOT_IN
  | ExpressionType.OPERATOR_ADD
  | ExpressionType.OPERATOR_SUBTRACT
  | ExpressionType.OPERATOR_MULTIPLY
  | ExpressionType.OPERATOR_DIVIDE
  | ExpressionType.OPERATOR_MOD
  | ExpressionType.OPERATOR_NEGATE
  | ExpressionType.OPERATOR_CONCAT;

/** A step into a JSON value after a column: `.field` or `[index]`. */
export type JsonPathSegment =
  | { readonly type: "field"; readonly name: string }
  | { readonly type: "index"; readonly value: number };

/**
 * `a`, `t.a`, or a JSON path `t.a.b[0].c`. The dotted names come first, in
 * `columnNames`; whether the first is a table is the binder's call. What
 * follows the first bracket is `path`, empty when there is none.
 */
export interface ColumnReferenceExpression {
  readonly expressionClass: ExpressionClass.COLUMN_REF;
  readonly alias: string | null;
  readonly columnNames: readonly string[];
  readonly path: readonly JsonPathSegment[];
}

/**
 * A literal and the type it has as written: `1` is INTEGER, `1.0` and `1e0`
 * REAL. A sign written before a number is part of it: `-1`.
 */
export type Literal =
  | { readonly type: "INTEGER" | "REAL"; readonly value: number }
  | { readonly type: "TEXT"; readonly value: string }
  | { readonly type: "BLOB"; readonly value: Uint8Array }
  | { readonly type: "BOOLEAN"; readonly value: boolean }
  | { readonly type: "NULL"; readonly value: null };

export type LiteralType = Literal["type"];

export type LiteralValue = Literal["value"];

export type ConstantExpression = {
  readonly expressionClass: ExpressionClass.CONSTANT;
  readonly alias: string | null;
} & Literal;

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
  readonly type: ComparisonType | ExpressionType.COMPARE_LIKE;
  readonly left: ParsedExpression;
  readonly right: ParsedExpression;
}

/** A chain of ANDs or of ORs, flattened: `a AND b AND c` has three children. */
export interface ConjunctionExpression {
  readonly expressionClass: ExpressionClass.CONJUNCTION;
  readonly alias: string | null;
  readonly type: ConjunctionType;
  readonly children: readonly ParsedExpression[];
}

/**
 * Arithmetic, NOT, IS [NOT] NULL, and [NOT] IN with a list (the left side
 * first). NOT IN and IS NOT NULL keep shapes of their own, as NOT EXISTS
 * does, so that printed text — index keys, column names — reads as written;
 * the binder makes each a NOT around the positive form.
 */
export interface OperatorExpression {
  readonly expressionClass: ExpressionClass.OPERATOR;
  readonly alias: string | null;
  readonly type: OperatorType;
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
  readonly isDistinct: boolean;
  readonly isStar: boolean;
}

/**
 * A subquery as a value (SCALAR), a test (EXISTS, NOT_EXISTS), or a
 * quantified comparison `child <comparisonType> ANY|ALL (subquery)`;
 * `x IN (subquery)` is `x = ANY (subquery)`.
 */
export type SubqueryExpression = {
  readonly expressionClass: ExpressionClass.SUBQUERY;
  readonly alias: string | null;
  readonly subquery: SelectStatement;
} & (
  | { readonly subqueryType: "SCALAR" | "EXISTS" | "NOT_EXISTS" }
  | {
      readonly subqueryType: "ANY" | "ALL";
      readonly child: ParsedExpression;
      readonly comparisonType: ComparisonType;
    }
);

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
  readonly table: string | null;
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
  readonly table: string;
  readonly alias: string | null;
}

export type JoinConstraint =
  | { readonly type: "ON"; readonly condition: ParsedExpression }
  | { readonly type: "USING"; readonly columns: readonly string[] };

/** A join; `FROM a, b` is a CROSS join, and has no constraint. */
export interface JoinReference {
  readonly type: TableReferenceType.JOIN;
  readonly left: TableReference;
  readonly right: TableReference;
  readonly joinType: JoinType;
  readonly constraint: JoinConstraint | null;
}

export interface SubqueryReference {
  readonly type: TableReferenceType.SUBQUERY;
  readonly subquery: SelectStatement;
  readonly alias: string | null;
}

export type TableReference =
  BaseTableReference | JoinReference | SubqueryReference;

// ============================================================================
// Queries
// ============================================================================

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

/** LIMIT and OFFSET; `LIMIT a, b` is `LIMIT b OFFSET a`. */
export interface LimitClause {
  readonly limit: ParsedExpression | null;
  readonly offset: ParsedExpression | null;
}

export interface CTENode {
  readonly name: string;
  /** Column names given after the CTE's name, if any. */
  readonly columnNames: readonly string[];
  readonly query: SelectStatement;
}

/** A WITH clause, its CTEs in the order written. */
export interface WithClause {
  readonly isRecursive: boolean;
  readonly ctes: readonly CTENode[];
}

export interface SelectNode {
  readonly type: "SELECT_NODE";
  readonly isDistinct: boolean;
  readonly selectList: readonly ParsedExpression[];
  readonly from: TableReference | null;
  readonly where: ParsedExpression | null;
  readonly groupBy: readonly ParsedExpression[];
  readonly having: ParsedExpression | null;
}

export enum SetOperationType {
  UNION = "UNION",
  UNION_ALL = "UNION_ALL",
  INTERSECT = "INTERSECT",
  EXCEPT = "EXCEPT",
}

/** A compound query; its operators bind left to right, as in SQLite. */
export interface SetOperationNode {
  readonly type: "SET_OPERATION_NODE";
  readonly setOperationType: SetOperationType;
  readonly left: SelectNode | SetOperationNode;
  readonly right: SelectNode;
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

/**
 * A query: an optional WITH, a body, and the ORDER BY and LIMIT that apply
 * to the whole body, compound or not.
 */
export interface SelectStatement {
  readonly type: StatementType.SELECT_STATEMENT;
  readonly with: WithClause | null;
  readonly body: SelectNode | SetOperationNode;
  readonly orderBy: readonly OrderByNode[];
  readonly limit: LimitClause | null;
}

/** `column = value` in UPDATE and in ON CONFLICT DO UPDATE. */
export interface Assignment {
  readonly column: string;
  readonly value: ParsedExpression;
}

export type OnConflictAction =
  | { readonly type: "NOTHING" }
  | {
      readonly type: "UPDATE";
      readonly assignments: readonly Assignment[];
      readonly where: ParsedExpression | null;
    };

export interface OnConflictClause {
  /** The columns in `ON CONFLICT (…)`, or null when none are given. */
  readonly target: readonly string[] | null;
  readonly action: OnConflictAction;
}

export type InsertSource =
  | {
      readonly type: "VALUES";
      readonly rows: readonly (readonly ParsedExpression[])[];
    }
  | { readonly type: "SELECT"; readonly query: SelectStatement };

export interface InsertStatement {
  readonly type: StatementType.INSERT_STATEMENT;
  readonly table: string;
  readonly columns: readonly string[];
  readonly source: InsertSource;
  readonly onConflict: OnConflictClause | null;
}

export interface UpdateStatement {
  readonly type: StatementType.UPDATE_STATEMENT;
  readonly table: string;
  readonly assignments: readonly Assignment[];
  readonly where: ParsedExpression | null;
}

export interface DeleteStatement {
  readonly type: StatementType.DELETE_STATEMENT;
  readonly table: string;
  readonly where: ParsedExpression | null;
}

/** A column constraint, as written; NULL says the column allows NULL. */
export type ColumnConstraint =
  | { readonly type: "PRIMARY KEY" }
  | { readonly type: "NOT NULL" }
  | { readonly type: "NULL" }
  | { readonly type: "UNIQUE" }
  | { readonly type: "AUTOINCREMENT" }
  | { readonly type: "DEFAULT"; readonly value: ParsedExpression };

export interface ColumnDefinition {
  readonly name: string;
  /** The declared type as written (`varchar(255)`), or null if none. */
  readonly type: string | null;
  /** In the order written; one may appear more than once. */
  readonly constraints: readonly ColumnConstraint[];
}

export interface CreateTableStatement {
  readonly type: StatementType.CREATE_TABLE_STATEMENT;
  readonly table: string;
  readonly ifNotExists: boolean;
  readonly columns: readonly ColumnDefinition[];
  /** The columns of each table-level `PRIMARY KEY (…)`, as written. */
  readonly primaryKeys: readonly (readonly string[])[];
}

export interface CreateIndexStatement {
  readonly type: StatementType.CREATE_INDEX_STATEMENT;
  readonly name: string;
  readonly table: string;
  readonly expressions: readonly ParsedExpression[];
  readonly isUnique: boolean;
  readonly ifNotExists: boolean;
}

export enum AlterType {
  ADD_COLUMN = "ADD_COLUMN",
  DROP_COLUMN = "DROP_COLUMN",
}

export type AlterTableStatement = {
  readonly type: StatementType.ALTER_TABLE_STATEMENT;
  readonly table: string;
} & (
  | {
      readonly alterType: AlterType.ADD_COLUMN;
      readonly column: ColumnDefinition;
    }
  | { readonly alterType: AlterType.DROP_COLUMN; readonly columnName: string }
);

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

/** When BEGIN takes the write lock: DEFERRED at the first write, IMMEDIATE at once. */
export type BeginMode = "DEFERRED" | "IMMEDIATE";

export type TransactionStatement =
  | {
      readonly type: StatementType.TRANSACTION_STATEMENT;
      readonly transactionType: TransactionType.BEGIN;
      readonly mode: BeginMode;
    }
  | {
      readonly type: StatementType.TRANSACTION_STATEMENT;
      readonly transactionType:
        TransactionType.COMMIT | TransactionType.ROLLBACK;
    };

export type BeginStatement = Extract<
  TransactionStatement,
  { transactionType: TransactionType.BEGIN }
>;

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
