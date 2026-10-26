// Test helpers: parse one statement of a known kind, and print syntax trees
// as S-expressions, so that a test states the tree it expects in one line.

import { expect } from "vitest";
import { NotSupportedError, SqlSyntaxError } from "../../../errors.js";
import { childExpressions, parse } from "../index.js";
import {
  ExpressionClass,
  ExpressionType,
  StatementType,
  TableReferenceType,
  type CaseExpression,
  type Literal,
  type LimitClause,
  type OrderByNode,
  type ParsedExpression,
  type SelectListItem,
  type SelectNode,
  type SelectStatement,
  type SetOperationNode,
  type Statement,
  type SubqueryExpression,
  type TableReference,
  type WithClause,
} from "../types.js";

export function only(sql: string): Statement {
  const statements = parse(sql);
  expect(statements).toHaveLength(1);
  const [statement] = statements;
  if (statement === undefined) throw new Error("no statement");
  return statement;
}

/** The one statement in `sql`, which must be of `type`. */
export function statementOf<Type extends StatementType>(
  sql: string,
  type: Type,
): Extract<Statement, { type: Type }> {
  const statement = only(sql);
  if (!isOfType(statement, type)) {
    throw new Error(`expected ${type} from ${sql}, got ${statement.type}`);
  }
  return statement;
}

function isOfType<Type extends StatementType>(
  statement: Statement,
  type: Type,
): statement is Extract<Statement, { type: Type }> {
  return statement.type === type;
}

export function query(sql: string): SelectStatement {
  return statementOf(sql, StatementType.SELECT_STATEMENT);
}

export function select(sql: string): SelectNode {
  const { body } = query(sql);
  if (body.type !== "SELECT_NODE") throw new Error(`compound: ${sql}`);
  return body;
}

/** The select list of `SELECT <list>`, printed. */
export function items(selectList: string): string[] {
  return select(`SELECT ${selectList}`).selectList.map((item) =>
    renderItem(item),
  );
}

/** The expression `SELECT <sql>` selects, printed. */
export function expression(sql: string): string {
  const printed = items(sql);
  expect(printed).toHaveLength(1);
  return printed[0] ?? "";
}

export function syntaxError(sql: string): SqlSyntaxError {
  const error = thrown(sql);
  if (!(error instanceof SqlSyntaxError)) {
    throw new TypeError(
      `expected a SqlSyntaxError from ${sql}, got ${String(error)}`,
    );
  }
  return error;
}

export function notSupported(sql: string): NotSupportedError {
  const error = thrown(sql);
  if (!(error instanceof NotSupportedError)) {
    throw new TypeError(
      `expected a NotSupportedError from ${sql}, got ${String(error)}`,
    );
  }
  return error;
}

function thrown(sql: string): unknown {
  try {
    parse(sql);
  } catch (error) {
    return error;
  }
  throw new Error(`parsed without an error: ${sql}`);
}

// --- Expressions ---------------------------------------------------------------

const SYMBOLS: Readonly<Record<ExpressionType, string>> = {
  [ExpressionType.COMPARE_EQUAL]: "=",
  [ExpressionType.COMPARE_NOTEQUAL]: "<>",
  [ExpressionType.COMPARE_LESSTHAN]: "<",
  [ExpressionType.COMPARE_LESSTHANOREQUALTO]: "<=",
  [ExpressionType.COMPARE_GREATERTHAN]: ">",
  [ExpressionType.COMPARE_GREATERTHANOREQUALTO]: ">=",
  [ExpressionType.COMPARE_LIKE]: "LIKE",
  [ExpressionType.CONJUNCTION_AND]: "AND",
  [ExpressionType.CONJUNCTION_OR]: "OR",
  [ExpressionType.OPERATOR_NOT]: "NOT",
  [ExpressionType.OPERATOR_IS_NULL]: "IS NULL",
  [ExpressionType.OPERATOR_IS_NOT_NULL]: "IS NOT NULL",
  [ExpressionType.OPERATOR_IN]: "IN",
  [ExpressionType.OPERATOR_NOT_IN]: "NOT IN",
  [ExpressionType.OPERATOR_ADD]: "+",
  [ExpressionType.OPERATOR_SUBTRACT]: "-",
  [ExpressionType.OPERATOR_MULTIPLY]: "*",
  [ExpressionType.OPERATOR_DIVIDE]: "/",
  [ExpressionType.OPERATOR_MOD]: "%",
  [ExpressionType.OPERATOR_NEGATE]: "NEGATE",
  [ExpressionType.OPERATOR_CONCAT]: "||",
};

function list(head: string, parts: readonly ParsedExpression[]): string {
  return `(${[head, ...parts.map((part) => render(part))].join(" ")})`;
}

/**
 * An expression as an S-expression: `(+ a 1)`, `(IS NULL a)`. Literals show
 * their type: `1`, `real(1)`, `'a'`, `NULL`, `TRUE`, `x'cafe'`; an alias
 * follows `AS`; a subquery inside is `<query>`.
 */
export function render(node: ParsedExpression): string {
  return renderNode(node);
}

/** A select list item, printed: its expression, then `AS alias` if it has one. */
export function renderItem(item: SelectListItem): string {
  const printed = render(item.expression);
  return item.alias === null ? printed : `${printed} AS ${item.alias}`;
}

/** An optional expression, printed, or null. */
export function renderOptional(node: ParsedExpression | null): string | null {
  return node === null ? null : render(node);
}

function renderNode(node: ParsedExpression): string {
  switch (node.expressionClass) {
    case ExpressionClass.COLUMN_REF: {
      const path = node.path.map((segment) =>
        segment.type === "field"
          ? `.${segment.name}`
          : `[${String(segment.value)}]`,
      );
      return node.names.join(".") + path.join("");
    }
    case ExpressionClass.CONSTANT: {
      return renderLiteral(node);
    }
    case ExpressionClass.PARAMETER: {
      return `$${String(node.index + 1)}`;
    }
    case ExpressionClass.COMPARISON: {
      return list(SYMBOLS[node.type], [node.left, node.right]);
    }
    case ExpressionClass.CONJUNCTION: {
      return list(SYMBOLS[node.type], node.children);
    }
    case ExpressionClass.OPERATOR: {
      return list(SYMBOLS[node.type], childExpressions(node));
    }
    case ExpressionClass.BETWEEN: {
      return list("BETWEEN", [node.input, node.lower, node.upper]);
    }
    case ExpressionClass.FUNCTION: {
      if (node.isStar) return `(${node.functionName} *)`;
      const head = node.isDistinct
        ? `${node.functionName} DISTINCT`
        : node.functionName;
      return list(head, node.children);
    }
    case ExpressionClass.SUBQUERY: {
      return renderSubquery(node);
    }
    case ExpressionClass.CASE: {
      return renderCase(node);
    }
    case ExpressionClass.CAST: {
      return `(CAST ${render(node.child)} '${node.castType}')`;
    }
    case ExpressionClass.STAR: {
      return node.table === null ? "*" : `${node.table}.*`;
    }
  }
}

function renderLiteral(literal: Literal): string {
  switch (literal.type) {
    case "INTEGER": {
      return String(literal.value);
    }
    case "REAL": {
      return `real(${String(literal.value)})`;
    }
    case "TEXT": {
      return `'${literal.value}'`;
    }
    case "BLOB": {
      return `x'${Buffer.from(literal.value).toString("hex")}'`;
    }
    case "BOOLEAN": {
      return literal.value ? "TRUE" : "FALSE";
    }
    case "NULL": {
      return "NULL";
    }
  }
}

function renderSubquery(node: SubqueryExpression): string {
  switch (node.subqueryType) {
    case "SCALAR":
    case "EXISTS":
    case "NOT_EXISTS": {
      return `(${node.subqueryType} <query>)`;
    }
    case "ANY":
    case "ALL": {
      const operator = SYMBOLS[node.comparisonType];
      return `(${operator} ${node.subqueryType} ${render(node.child)} <query>)`;
    }
  }
}

function renderCase(node: CaseExpression): string {
  const operand = node.operand === null ? "" : ` ${render(node.operand)}`;
  const checks = node.caseChecks.map(
    (check) => ` ${list("WHEN", [check.whenExpression, check.thenExpression])}`,
  );
  const otherwise =
    node.elseExpression === null
      ? ""
      : ` ${list("ELSE", [node.elseExpression])}`;
  return `(CASE${operand}${checks.join("")}${otherwise})`;
}

// --- Queries and tables --------------------------------------------------------------

/**
 * A query, compound or not: `WITH a AS {…} (SELECT … FROM … WHERE …) ORDER
 * BY … LIMIT …`, a compound as `(UNION_ALL left right)`, a nested query in
 * braces.
 */
export function renderQuery(statement: SelectStatement): string {
  const parts = [
    statement.with === null ? "" : renderWith(statement.with),
    renderBody(statement.body),
    renderOrderBy(statement.orderBy),
    statement.limit === null ? "" : renderLimit(statement.limit),
  ];
  return parts.filter((part) => part !== "").join(" ");
}

function renderWith(clause: WithClause): string {
  const ctes = clause.ctes.map((cte) => {
    const columns =
      cte.columnNames.length === 0 ? "" : `(${cte.columnNames.join(", ")})`;
    return `${cte.name}${columns} AS {${renderQuery(cte.query)}}`;
  });
  const recursive = clause.isRecursive ? "RECURSIVE " : "";
  return `WITH ${recursive}${ctes.join(", ")}`;
}

function renderBody(body: SelectNode | SetOperationNode): string {
  if (body.type === "SET_OPERATION_NODE") {
    const sides = `${renderBody(body.left)} ${renderBody(body.right)}`;
    return `(${body.setOperationType} ${sides})`;
  }
  const clauses = [
    `SELECT${body.isDistinct ? " DISTINCT" : ""}`,
    body.selectList.map((item) => renderItem(item)).join(", "),
    body.from === null ? "" : `FROM ${renderTable(body.from)}`,
    body.where === null ? "" : `WHERE ${render(body.where)}`,
    body.groupBy.length === 0
      ? ""
      : `GROUP BY ${body.groupBy.map((group) => render(group)).join(", ")}`,
    body.having === null ? "" : `HAVING ${render(body.having)}`,
  ];
  return `(${clauses.filter((clause) => clause !== "").join(" ")})`;
}

function renderOrderBy(orders: readonly OrderByNode[]): string {
  if (orders.length === 0) return "";
  const orderItems = orders.map((order) => {
    const direction = order.direction === "descending" ? " DESC" : "";
    const nulls = order.nullOrder === null ? "" : ` ${order.nullOrder}`;
    return `${render(order.expression)}${direction}${nulls}`;
  });
  return `ORDER BY ${orderItems.join(", ")}`;
}

function renderLimit(clause: LimitClause): string {
  const limit = clause.limit === null ? "" : `LIMIT ${render(clause.limit)}`;
  const offset =
    clause.offset === null ? "" : `OFFSET ${render(clause.offset)}`;
  return [limit, offset].filter((part) => part !== "").join(" ");
}

/** A FROM clause as a tree: `(CROSS a b)`, `(LEFT a b ON (= x y))`. */
export function renderTable(table: TableReference): string {
  switch (table.type) {
    case TableReferenceType.BASE_TABLE: {
      return table.alias === null
        ? table.table
        : `${table.table} AS ${table.alias}`;
    }
    case TableReferenceType.SUBQUERY: {
      return `{${renderQuery(table.subquery)}} AS ${table.alias ?? "-"}`;
    }
    case TableReferenceType.JOIN: {
      const sides = `${renderTable(table.left)} ${renderTable(table.right)}`;
      return `(${table.joinType} ${sides}${renderConstraint(table)})`;
    }
  }
}

function renderConstraint(
  join: Extract<TableReference, { type: TableReferenceType.JOIN }>,
): string {
  if (join.constraint === null) return "";
  return join.constraint.type === "ON"
    ? ` ON ${render(join.constraint.condition)}`
    : ` USING ${join.constraint.columns.join(",")}`;
}
