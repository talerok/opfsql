// Test helpers: parse one statement of a known kind, and print expressions
// as S-expressions so that a test states the tree it expects in one line.

import { expect } from "vitest";
import { NotSupportedError, SqlSyntaxError } from "../../../errors.js";
import { parse } from "../index.js";
import {
  ExpressionClass,
  ExpressionType,
  StatementType,
  type CaseExpression,
  type LiteralValue,
  type ParsedExpression,
  type SelectNode,
  type SelectStatement,
  type Statement,
} from "../types.js";

export function only(sql: string): Statement {
  const statements = parse(sql);
  expect(statements).toHaveLength(1);
  const [statement] = statements;
  if (statement === undefined) throw new Error("no statement");
  return statement;
}

export function query(sql: string): SelectStatement {
  const statement = only(sql);
  if (statement.type !== StatementType.SELECT_STATEMENT) {
    throw new Error(`not a query: ${sql}`);
  }
  return statement;
}

export function select(sql: string): SelectNode {
  const { node } = query(sql);
  if (node.type !== "SELECT_NODE") throw new Error(`compound: ${sql}`);
  return node;
}

/** The select list of `SELECT <list>`, printed. */
export function items(selectList: string): string[] {
  return select(`SELECT ${selectList}`).selectList.map((item) => render(item));
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

const SYMBOLS: Readonly<Partial<Record<ExpressionType, string>>> = {
  [ExpressionType.COMPARE_EQUAL]: "=",
  [ExpressionType.COMPARE_NOTEQUAL]: "<>",
  [ExpressionType.COMPARE_LESSTHAN]: "<",
  [ExpressionType.COMPARE_LESSTHANOREQUALTO]: "<=",
  [ExpressionType.COMPARE_GREATERTHAN]: ">",
  [ExpressionType.COMPARE_GREATERTHANOREQUALTO]: ">=",
  [ExpressionType.COMPARE_LIKE]: "LIKE",
  [ExpressionType.COMPARE_NOT_LIKE]: "NOT LIKE",
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
 * follows `AS`.
 */
export function render(node: ParsedExpression): string {
  const printed = renderNode(node);
  return node.alias === null ? printed : `${printed} AS ${node.alias}`;
}

function renderNode(node: ParsedExpression): string {
  switch (node.expressionClass) {
    case ExpressionClass.COLUMN_REF: {
      const path = (node.path ?? []).map((segment) =>
        segment.type === "field"
          ? `.${segment.name}`
          : `[${String(segment.value)}]`,
      );
      return node.columnNames.join(".") + path.join("");
    }
    case ExpressionClass.CONSTANT: {
      return renderConstant(node.type, node.value);
    }
    case ExpressionClass.PARAMETER: {
      return `$${String(node.index + 1)}`;
    }
    case ExpressionClass.COMPARISON: {
      return list(SYMBOLS[node.type] ?? node.type, [node.left, node.right]);
    }
    case ExpressionClass.CONJUNCTION:
    case ExpressionClass.OPERATOR: {
      return list(SYMBOLS[node.type] ?? node.type, node.children);
    }
    case ExpressionClass.BETWEEN: {
      return list("BETWEEN", [node.input, node.lower, node.upper]);
    }
    case ExpressionClass.FUNCTION: {
      if (node.isStar) return `(${node.functionName} *)`;
      const head = node.distinct
        ? `${node.functionName} DISTINCT`
        : node.functionName;
      return list(head, node.children);
    }
    case ExpressionClass.SUBQUERY: {
      return renderSubquery(node.subqueryType, node.child, node.comparisonType);
    }
    case ExpressionClass.CASE: {
      return renderCase(node);
    }
    case ExpressionClass.CAST: {
      return `(CAST ${render(node.child)} '${node.castType}')`;
    }
    case ExpressionClass.STAR: {
      return node.tableName === null ? "*" : `${node.tableName}.*`;
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

/** An optional expression, printed, or null. */
export function renderOptional(node: ParsedExpression | null): string | null {
  return node === null ? null : render(node);
}

function renderConstant(type: string, value: LiteralValue): string {
  if (value instanceof Uint8Array)
    return `x'${Buffer.from(value).toString("hex")}'`;
  if (typeof value === "string") return `'${value}'`;
  if (value === null) return "NULL";
  if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
  const number = String(value);
  return type === "REAL" ? `real(${number})` : number;
}

function renderSubquery(
  type: string,
  child: ParsedExpression | null,
  comparison: ExpressionType | undefined,
): string {
  if (child === null) return `(${type} <query>)`;
  const operator =
    comparison === undefined ? "?" : (SYMBOLS[comparison] ?? comparison);
  return `(${operator} ${type} ${render(child)} <query>)`;
}
