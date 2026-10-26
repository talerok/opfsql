import { realLiteral, sqlLiteral } from "../../values/index.js";
import { compoundMembers } from "./compound.js";
import { keywordOf } from "./keywords.js";
import { above, power } from "../precedence.js";
import { comparisonPower, OPERATOR_POWER, powerOf } from "./printed-power.js";
import {
  ExpressionClass,
  ExpressionType,
  JoinType,
  OrderByNullType,
  SetOperationType,
  TableReferenceType,
  type CaseExpression,
  type ColumnReferenceExpression,
  type ComparisonType,
  type CTENode,
  type JoinReference,
  type JsonPathSegment,
  type LimitClause,
  type Literal,
  type BinaryOperatorType,
  type OperatorExpression,
  type OrderByNode,
  type ParsedExpression,
  type SelectListItem,
  type SelectNode,
  type SelectStatement,
  type SetOperationNode,
  type SubqueryExpression,
  type TableReference,
} from "./types.js";

// ---------------------------------------------------------------------------
// The printer: syntax trees back to SQL text. What it prints parses to the
// tree it was given, so printing is idempotent: parse(format(x)) prints the
// same. It is the canonical text of an expression — how CREATE INDEX stores
// an expression key, and how a result column without an alias is named.
// Names are quoted when they are not plain identifiers or are keywords;
// parentheses go where the parser's precedence (precedence.ts) needs them.
// ---------------------------------------------------------------------------

const PLAIN_NAME = /^[A-Za-z_][\w$]*$/;

/** `expression` as SQL text that parses back to it. Aliases are not printed. */
export function formatExpression(expression: ParsedExpression): string {
  switch (expression.expressionClass) {
    case ExpressionClass.COLUMN_REF: {
      return formatColumn(expression);
    }
    case ExpressionClass.CONSTANT: {
      return formatLiteral(expression);
    }
    case ExpressionClass.PARAMETER: {
      return `$${String(expression.index + 1)}`;
    }
    case ExpressionClass.COMPARISON: {
      const operator = COMPARISON_TEXT[expression.type];
      const level = comparisonPower(expression.type);
      return binary(expression.left, operator, expression.right, level);
    }
    case ExpressionClass.CONJUNCTION: {
      return formatConjunction(expression.type, expression.children);
    }
    case ExpressionClass.OPERATOR: {
      return formatOperator(expression);
    }
    case ExpressionClass.BETWEEN: {
      const input = operand(expression.input, power.equality);
      const lower = operand(expression.lower, above(power.equality));
      const upper = operand(expression.upper, above(power.equality));
      return `${input} BETWEEN ${lower} AND ${upper}`;
    }
    case ExpressionClass.FUNCTION: {
      return formatCall(expression);
    }
    case ExpressionClass.SUBQUERY: {
      return formatSubquery(expression);
    }
    case ExpressionClass.CASE: {
      return formatCase(expression);
    }
    case ExpressionClass.CAST: {
      return `CAST(${formatExpression(expression.child)} AS ${expression.castType})`;
    }
    case ExpressionClass.STAR: {
      const table = expression.table;
      return table === null ? "*" : `${formatName(table)}.*`;
    }
  }
}

/** A name as SQL writes it: bare when that reads back as the same name. */
export function formatName(name: string): string {
  const isBare = PLAIN_NAME.test(name) && keywordOf(name) === undefined;
  return isBare ? name : `"${name.replaceAll('"', '""')}"`;
}

// --- Precedence ----------------------------------------------------------------

/** `expression`, in parentheses unless it binds at least as tightly as `minimum`. */
function operand(expression: ParsedExpression, minimum: number): string {
  const text = formatExpression(expression);
  return powerOf(expression) >= minimum ? text : `(${text})`;
}

/** A left-associative operator: the right side binds tighter than it. */
function binary(
  left: ParsedExpression,
  operator: string,
  right: ParsedExpression,
  level: number,
): string {
  return `${operand(left, level)} ${operator} ${operand(right, above(level))}`;
}

// --- Operators -----------------------------------------------------------------

const COMPARISON_TEXT: Readonly<
  Record<ComparisonType | ExpressionType.COMPARE_LIKE, string>
> = {
  [ExpressionType.COMPARE_EQUAL]: "=",
  [ExpressionType.COMPARE_NOTEQUAL]: "<>",
  [ExpressionType.COMPARE_LESSTHAN]: "<",
  [ExpressionType.COMPARE_LESSTHANOREQUALTO]: "<=",
  [ExpressionType.COMPARE_GREATERTHAN]: ">",
  [ExpressionType.COMPARE_GREATERTHANOREQUALTO]: ">=",
  [ExpressionType.COMPARE_LIKE]: "LIKE",
};

function formatOperator(expression: OperatorExpression): string {
  const level = OPERATOR_POWER[expression.type];
  switch (expression.type) {
    case ExpressionType.OPERATOR_NOT: {
      return `NOT ${operand(expression.operand, level)}`;
    }
    case ExpressionType.OPERATOR_NEGATE: {
      return `-${minusOperand(expression.operand)}`;
    }
    case ExpressionType.OPERATOR_IS_NULL: {
      return `${operand(expression.operand, level)} IS NULL`;
    }
    case ExpressionType.OPERATOR_IS_NOT_NULL: {
      return `${operand(expression.operand, level)} IS NOT NULL`;
    }
    case ExpressionType.OPERATOR_IN: {
      const { needle, list } = expression;
      return `${operand(needle, level)} IN (${formatList(list)})`;
    }
    case ExpressionType.OPERATOR_NOT_IN: {
      const { needle, list } = expression;
      return `${operand(needle, level)} NOT IN (${formatList(list)})`;
    }
    case ExpressionType.OPERATOR_ADD:
    case ExpressionType.OPERATOR_SUBTRACT:
    case ExpressionType.OPERATOR_MULTIPLY:
    case ExpressionType.OPERATOR_DIVIDE:
    case ExpressionType.OPERATOR_MOD:
    case ExpressionType.OPERATOR_CONCAT: {
      const { left, right, type } = expression;
      return binary(left, BINARY_SYMBOL[type], right, level);
    }
  }
}

const BINARY_SYMBOL: Record<BinaryOperatorType, string> = {
  [ExpressionType.OPERATOR_ADD]: "+",
  [ExpressionType.OPERATOR_SUBTRACT]: "-",
  [ExpressionType.OPERATOR_MULTIPLY]: "*",
  [ExpressionType.OPERATOR_DIVIDE]: "/",
  [ExpressionType.OPERATOR_MOD]: "%",
  [ExpressionType.OPERATOR_CONCAT]: "||",
};

/**
 * The operand of a prefix `-`. A number goes in parentheses, since `-5` is
 * a literal of its own, and so does a second `-`, since `--` starts a
 * comment.
 */
function minusOperand(expression: ParsedExpression): string {
  const isNumber =
    expression.expressionClass === ExpressionClass.CONSTANT &&
    (expression.type === "INTEGER" || expression.type === "REAL");
  const isNegation =
    expression.expressionClass === ExpressionClass.OPERATOR &&
    expression.type === ExpressionType.OPERATOR_NEGATE;
  const text = formatExpression(expression);
  const isBare = !isNumber && !isNegation && powerOf(expression) >= power.unary;
  return isBare ? text : `(${text})`;
}

/** `a AND b AND c`; a nested AND or OR keeps its parentheses. */
function formatConjunction(
  type: ExpressionType.CONJUNCTION_AND | ExpressionType.CONJUNCTION_OR,
  children: readonly ParsedExpression[],
): string {
  const isAnd = type === ExpressionType.CONJUNCTION_AND;
  const level = isAnd ? power.and : power.or;
  const parts = children.map((child) => operand(child, above(level)));
  return parts.join(isAnd ? " AND " : " OR ");
}

function formatList(expressions: readonly ParsedExpression[]): string {
  return expressions
    .map((expression) => formatExpression(expression))
    .join(", ");
}

// --- Primaries -----------------------------------------------------------------

function formatColumn(reference: ColumnReferenceExpression): string {
  const names = reference.names.map((name) => formatName(name));
  const path = reference.path.map((segment) => formatSegment(segment));
  return names.join(".") + path.join("");
}

function formatSegment(segment: JsonPathSegment): string {
  if (segment.type === "field") return `.${formatName(segment.name)}`;
  return `[${String(segment.value)}]`;
}

function formatLiteral(literal: Literal): string {
  if (literal.type === "REAL") return realLiteral(literal.value);
  return sqlLiteral(literal.value);
}

function formatCall(
  call: Extract<
    ParsedExpression,
    { expressionClass: ExpressionClass.FUNCTION }
  >,
): string {
  const name = formatName(call.functionName);
  if (call.isStar) return `${name}(*)`;
  const distinct = call.isDistinct ? "DISTINCT " : "";
  return `${name}(${distinct}${formatList(call.children)})`;
}

function formatSubquery(expression: SubqueryExpression): string {
  const query = `(${formatQuery(expression.subquery)})`;
  switch (expression.subqueryType) {
    case "SCALAR": {
      return query;
    }
    case "EXISTS": {
      return `EXISTS ${query}`;
    }
    case "NOT_EXISTS": {
      return `NOT EXISTS ${query}`;
    }
    case "ANY":
    case "ALL": {
      const level = comparisonPower(expression.comparisonType);
      const needle = operand(expression.child, level);
      return `${needle} ${quantified(expression)} ${query}`;
    }
  }
}

/** `IN` for `= ANY`, as the parser reads it; `<op> ANY` or `<op> ALL` otherwise. */
function quantified(
  expression: Extract<SubqueryExpression, { subqueryType: "ANY" | "ALL" }>,
): string {
  const isIn =
    expression.subqueryType === "ANY" &&
    expression.comparisonType === ExpressionType.COMPARE_EQUAL;
  if (isIn) return "IN";
  return `${COMPARISON_TEXT[expression.comparisonType]} ${expression.subqueryType}`;
}

function formatCase(expression: CaseExpression): string {
  const parts = ["CASE"];
  if (expression.operand !== null) {
    parts.push(formatExpression(expression.operand));
  }
  for (const check of expression.caseChecks) {
    const when = formatExpression(check.whenExpression);
    parts.push(`WHEN ${when} THEN ${formatExpression(check.thenExpression)}`);
  }
  if (expression.elseExpression !== null) {
    parts.push(`ELSE ${formatExpression(expression.elseExpression)}`);
  }
  parts.push("END");
  return parts.join(" ");
}

// --- Queries -------------------------------------------------------------------

/** A query as SQL text that parses back to it. */
export function formatQuery(statement: SelectStatement): string {
  const parts = [
    statement.with === null ? "" : formatWith(statement.with),
    formatBody(statement.body),
    formatOrderBy(statement.orderBy),
    statement.limit === null ? "" : formatLimit(statement.limit),
  ];
  return parts.filter((part) => part !== "").join(" ");
}

function formatWith(clause: NonNullable<SelectStatement["with"]>): string {
  const recursive = clause.isRecursive ? "RECURSIVE " : "";
  const ctes = clause.ctes.map((cte) => formatCte(cte));
  return `WITH ${recursive}${ctes.join(", ")}`;
}

function formatCte(cte: CTENode): string {
  const columns = cte.columnNames.map((name) => formatName(name));
  const list = columns.length === 0 ? "" : `(${columns.join(", ")})`;
  return `${formatName(cte.name)}${list} AS (${formatQuery(cte.query)})`;
}

const SET_OPERATOR_TEXT: Readonly<Record<SetOperationType, string>> = {
  [SetOperationType.UNION]: "UNION",
  [SetOperationType.UNION_ALL]: "UNION ALL",
  [SetOperationType.INTERSECT]: "INTERSECT",
  [SetOperationType.EXCEPT]: "EXCEPT",
};

function formatBody(body: SelectNode | SetOperationNode): string {
  const { first, links } = compoundMembers(body);
  const joined = links.map(
    (link) =>
      `${SET_OPERATOR_TEXT[link.operator]} ${formatSelect(link.select)}`,
  );
  return [formatSelect(first), ...joined].join(" ");
}

function formatSelect(node: SelectNode): string {
  const distinct = node.isDistinct ? "DISTINCT " : "";
  const parts = [`SELECT ${distinct}${formatSelectList(node.selectList)}`];
  if (node.from !== null) parts.push(`FROM ${formatTable(node.from)}`);
  if (node.where !== null) parts.push(`WHERE ${formatExpression(node.where)}`);
  if (node.groupBy.length > 0)
    parts.push(`GROUP BY ${formatList(node.groupBy)}`);
  if (node.having !== null) {
    parts.push(`HAVING ${formatExpression(node.having)}`);
  }
  return parts.join(" ");
}

function formatSelectList(items: readonly SelectListItem[]): string {
  return items
    .map((item) => formatExpression(item.expression) + aliasText(item.alias))
    .join(", ");
}

function aliasText(alias: string | null): string {
  return alias === null ? "" : ` AS ${formatName(alias)}`;
}

function formatTable(table: TableReference): string {
  switch (table.type) {
    case TableReferenceType.BASE_TABLE: {
      return formatName(table.table) + aliasText(table.alias);
    }
    case TableReferenceType.SUBQUERY: {
      return `(${formatQuery(table.subquery)})${aliasText(table.alias)}`;
    }
    case TableReferenceType.JOIN: {
      return formatJoin(table);
    }
  }
}

const JOIN_TEXT: Readonly<Record<JoinType, string>> = {
  [JoinType.INNER]: "JOIN",
  [JoinType.LEFT]: "LEFT JOIN",
  [JoinType.RIGHT]: "RIGHT JOIN",
  [JoinType.CROSS]: "CROSS JOIN",
};

function formatJoin(join: JoinReference): string {
  const sides = `${formatTable(join.left)} ${JOIN_TEXT[join.joinType]} ${formatTable(join.right)}`;
  const { constraint } = join;
  if (constraint === null) return sides;
  if (constraint.type === "ON") {
    return `${sides} ON ${formatExpression(constraint.condition)}`;
  }
  const columns = constraint.columns.map((name) => formatName(name));
  return `${sides} USING (${columns.join(", ")})`;
}

function formatOrderBy(orders: readonly OrderByNode[]): string {
  if (orders.length === 0) return "";
  const keys = orders.map((order) => {
    const direction = order.direction === "descending" ? " DESC" : "";
    return `${formatExpression(order.expression)}${direction}${nullsText(order)}`;
  });
  return `ORDER BY ${keys.join(", ")}`;
}

function nullsText(order: OrderByNode): string {
  switch (order.nullOrder) {
    case null: {
      return "";
    }
    case OrderByNullType.NULLS_FIRST: {
      return " NULLS FIRST";
    }
    case OrderByNullType.NULLS_LAST: {
      return " NULLS LAST";
    }
  }
}

function formatLimit(clause: LimitClause): string {
  const parts: string[] = [];
  if (clause.limit !== null) {
    parts.push(`LIMIT ${formatExpression(clause.limit)}`);
  }
  if (clause.offset !== null) {
    parts.push(`OFFSET ${formatExpression(clause.offset)}`);
  }
  return parts.join(" ");
}
