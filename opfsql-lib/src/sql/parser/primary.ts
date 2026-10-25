// ---------------------------------------------------------------------------
// What an expression starts with: literals, parameters, names and calls,
// prefix operators, parentheses, subqueries, CASE and CAST.
// ---------------------------------------------------------------------------

import { NotSupportedError } from "../../errors.js";
import { hexToBytes } from "../../values/index.js";
import type { Cursor } from "./cursor.js";
import type { Keyword } from "./keywords.js";
import { constantNode, notNode, operatorNode } from "./nodes.js";
import { power } from "./precedence.js";
import type { SymbolText, Token } from "./tokens.js";
import { parseTypeName } from "./type-name.js";
import {
  ExpressionClass,
  ExpressionType,
  type CaseCheck,
  type CaseExpression,
  type ColumnReferenceExpression,
  type FunctionExpression,
  type JsonPathSegment,
  type ParsedExpression,
  type SelectStatement,
  type SubqueryExpression,
} from "./types.js";

export function parsePrefix(cursor: Cursor): ParsedExpression {
  const token = cursor.peek();
  switch (token.kind) {
    case "integer":
    case "real": {
      return parseNumber(cursor, null);
    }
    case "string": {
      cursor.next();
      return constantNode("TEXT", token.value);
    }
    case "blob": {
      cursor.next();
      return constantNode("BLOB", hexToBytes(token.value));
    }
    case "parameter": {
      cursor.next();
      const index = cursor.parameterIndex(token);
      return { expressionClass: ExpressionClass.PARAMETER, alias: null, index };
    }
    case "keyword": {
      return parseKeyword(cursor, token.value);
    }
    case "symbol": {
      return parseSymbol(cursor, token.value);
    }
    case "identifier":
    case "quoted": {
      return parseName(cursor);
    }
    case "end": {
      return cursor.fail("an expression");
    }
  }
}

/** True if a query starts here: `SELECT …` or `WITH …`. */
export function startsQuery(cursor: Cursor, offset = 0): boolean {
  return cursor.isKeyword("SELECT", offset) || cursor.isKeyword("WITH", offset);
}

/** `( SELECT … )` */
export function parseSubquery(cursor: Cursor): SelectStatement {
  cursor.expectSymbol("(");
  if (!startsQuery(cursor)) cursor.fail("a subquery");
  const query = cursor.query();
  cursor.expectSymbol(")");
  return query;
}

// --- Literals ----------------------------------------------------------------

/** A number, with the sign written before it if any: `-1` is a literal. */
function parseNumber(cursor: Cursor, sign: Token | null): ParsedExpression {
  const token = cursor.next();
  const isNegative = sign?.value === "-";
  if (token.kind === "real") {
    const value = Number(token.value);
    return constantNode("REAL", isNegative ? 0 - value : value);
  }
  const value = integerValue(cursor, token, sign);
  return constantNode("INTEGER", isNegative ? 0 - value : value);
}

/**
 * The value of an integer literal, which must be a safe integer
 * (docs/semantics.md). Hex literals are 64-bit two's complement, as in
 * SQLite: 0xFFFFFFFFFFFFFFFF is -1.
 */
function integerValue(
  cursor: Cursor,
  token: Token,
  sign: Token | null = null,
): number {
  const isHex = /^0x/i.test(token.text);
  // SQLite allows 16 hex digits after any leading zeros.
  if (isHex && token.text.slice(2).replace(/^0+/, "").length > 16) {
    cursor.failAt(token, `hex literal ${token.text} has more than 16 digits`);
  }
  const value = Number(
    isHex ? BigInt.asIntN(64, BigInt(token.text)) : token.text,
  );
  if (!Number.isSafeInteger(value)) {
    const written = `${sign?.value ?? ""}${token.text}`;
    cursor.failAt(
      token,
      `integer ${written} is outside the safe range ±(2^53 - 1)`,
      sign?.start,
    );
  }
  return value;
}

// --- Keywords and symbols ----------------------------------------------------

const KEYWORD_PREFIXES: ReadonlyMap<
  Keyword,
  (cursor: Cursor) => ParsedExpression
> = new Map<Keyword, (cursor: Cursor) => ParsedExpression>([
  ["NULL", (cursor) => keywordLiteral(cursor, null)],
  ["TRUE", (cursor) => keywordLiteral(cursor, true)],
  ["FALSE", (cursor) => keywordLiteral(cursor, false)],
  ["NOT", parseNot],
  ["EXISTS", parseExists],
  ["CASE", parseCase],
  ["CAST", parseCast],
  ["CURRENT_DATE", unsupported("CURRENT_DATE")],
  ["CURRENT_TIME", unsupported("CURRENT_TIME")],
  ["CURRENT_TIMESTAMP", unsupported("CURRENT_TIMESTAMP")],
]);

function parseKeyword(cursor: Cursor, keyword: Keyword): ParsedExpression {
  const parse = KEYWORD_PREFIXES.get(keyword);
  if (parse !== undefined) return parse(cursor);
  return cursor.isName() ? parseName(cursor) : cursor.fail("an expression");
}

function parseSymbol(cursor: Cursor, symbol: SymbolText): ParsedExpression {
  if (symbol === "(") return parseParenthesized(cursor);
  if (symbol === "-" || symbol === "+") return parseSigned(cursor);
  if (symbol === "~") throw new NotSupportedError("the ~ operator");
  if (symbol === "[") throw new NotSupportedError("a name in brackets");
  if (symbol === "*") {
    cursor.failAt(
      cursor.peek(),
      "'*' is allowed only as a select list item and in count(*)",
    );
  }
  return cursor.fail("an expression");
}

function keywordLiteral(
  cursor: Cursor,
  value: boolean | null,
): ParsedExpression {
  cursor.next();
  return constantNode(value === null ? "NULL" : "BOOLEAN", value);
}

function unsupported(feature: string): () => never {
  return () => {
    throw new NotSupportedError(feature);
  };
}

/** `-x`, `+x`; a sign before a number is folded into the literal. */
function parseSigned(cursor: Cursor): ParsedExpression {
  const sign = cursor.next();
  const isNegative = sign.value === "-";
  const kind = cursor.peek().kind;
  if (kind === "integer" || kind === "real") return parseNumber(cursor, sign);
  const operand = cursor.expression(power.unary);
  return isNegative
    ? operatorNode(ExpressionType.OPERATOR_NEGATE, [operand])
    : operand;
}

/** `NOT x`; `NOT EXISTS (…)` is one node. */
function parseNot(cursor: Cursor): ParsedExpression {
  cursor.next();
  const operand = cursor.expression(power.not);
  const isExists =
    operand.expressionClass === ExpressionClass.SUBQUERY &&
    operand.subqueryType === "EXISTS";
  return isExists
    ? { ...operand, subqueryType: "NOT_EXISTS" }
    : notNode(operand);
}

function parseExists(cursor: Cursor): SubqueryExpression {
  cursor.next();
  return {
    expressionClass: ExpressionClass.SUBQUERY,
    alias: null,
    subqueryType: "EXISTS",
    subquery: parseSubquery(cursor),
    child: null,
  };
}

/** `( expression )` or a scalar subquery `( SELECT … )`. */
function parseParenthesized(cursor: Cursor): ParsedExpression {
  if (startsQuery(cursor, 1)) {
    return {
      expressionClass: ExpressionClass.SUBQUERY,
      alias: null,
      subqueryType: "SCALAR",
      subquery: parseSubquery(cursor),
      child: null,
    };
  }
  cursor.next();
  if (cursor.isKeyword("VALUES"))
    throw new NotSupportedError("VALUES as a query");
  const expression = cursor.expression();
  if (cursor.isSymbol(",")) throw new NotSupportedError("a row value");
  cursor.expectSymbol(")");
  return expression;
}

// --- CASE and CAST -------------------------------------------------------------

function parseCase(cursor: Cursor): CaseExpression {
  cursor.next();
  const operand = cursor.isKeyword("WHEN") ? null : cursor.expression();
  const caseChecks: CaseCheck[] = [];
  cursor.expectKeyword("WHEN");
  do {
    const whenExpression = cursor.expression();
    cursor.expectKeyword("THEN");
    caseChecks.push({ whenExpression, thenExpression: cursor.expression() });
  } while (cursor.matchKeyword("WHEN"));
  const elseExpression = cursor.matchKeyword("ELSE")
    ? cursor.expression()
    : null;
  cursor.expectKeyword("END");
  return {
    expressionClass: ExpressionClass.CASE,
    alias: null,
    operand,
    caseChecks,
    elseExpression,
  };
}

function parseCast(cursor: Cursor): ParsedExpression {
  cursor.next();
  cursor.expectSymbol("(");
  const child = cursor.expression();
  cursor.expectKeyword("AS");
  const castType = parseTypeName(cursor);
  cursor.expectSymbol(")");
  return {
    expressionClass: ExpressionClass.CAST,
    alias: null,
    child,
    castType,
  };
}

// --- Names: columns, JSON paths and calls --------------------------------------

/** `a`, `t.a`, `t.a.b[0].c` or `f(…)`. */
function parseName(cursor: Cursor): ParsedExpression {
  const name = cursor.name("an expression");
  if (cursor.isSymbol("(")) return parseCall(cursor, name);
  const columnNames = [name];
  while (cursor.matchSymbol(".")) {
    columnNames.push(cursor.label("a name after '.'"));
  }
  const reference: ColumnReferenceExpression = {
    expressionClass: ExpressionClass.COLUMN_REF,
    alias: null,
    columnNames,
  };
  const path = parseJsonPath(cursor);
  return path.length > 0 ? { ...reference, path } : reference;
}

/** `[0].field[1]…` after a column. */
function parseJsonPath(cursor: Cursor): JsonPathSegment[] {
  const segments: JsonPathSegment[] = [];
  while (
    cursor.isSymbol("[") ||
    (segments.length > 0 && cursor.isSymbol("."))
  ) {
    if (cursor.matchSymbol(".")) {
      segments.push({ type: "field", name: cursor.label("a field name") });
      continue;
    }
    cursor.next();
    if (cursor.peek().kind !== "integer") cursor.fail("an array index");
    segments.push({
      type: "index",
      value: integerValue(cursor, cursor.next()),
    });
    cursor.expectSymbol("]");
  }
  return segments;
}

function parseCall(cursor: Cursor, name: string): FunctionExpression {
  cursor.expectSymbol("(");
  const functionName = name.toLowerCase();
  const isStar = functionName === "count" && cursor.matchSymbol("*");
  const isDistinct = !isStar && cursor.matchKeyword("DISTINCT");
  // ALL is the default, as in SELECT ALL.
  const isAll = !isStar && !isDistinct && cursor.matchKeyword("ALL");
  const hasArguments =
    !isStar && (isDistinct || isAll || !cursor.isSymbol(")"));
  const children = hasArguments ? cursor.list(() => cursor.expression()) : [];
  if (cursor.isKeyword("ORDER")) {
    throw new NotSupportedError("ORDER BY in a function call");
  }
  cursor.expectSymbol(")");
  if (cursor.isKeyword("OVER")) {
    throw new NotSupportedError("OVER (window functions)");
  }
  if (cursor.isKeyword("FILTER")) {
    throw new NotSupportedError("FILTER");
  }
  return {
    expressionClass: ExpressionClass.FUNCTION,
    alias: null,
    functionName,
    children,
    distinct: isDistinct,
    isStar,
  };
}
