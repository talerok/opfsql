// ---------------------------------------------------------------------------
// What an expression starts with: literals, parameters, names and calls,
// prefix operators, parentheses, subqueries, CASE and CAST.
// ---------------------------------------------------------------------------

import { NotSupportedError } from "../../errors.js";
import { hexToBytes } from "../../values/index.js";
import type { Cursor } from "./cursor.js";
import type { Keyword } from "./keywords.js";
import { constantNode, notNode, operatorNode, subqueryNode } from "./nodes.js";
import { power } from "../precedence.js";
import type { SymbolText, Token } from "./tokens.js";
import { parseTypeName } from "./type-name.js";
import {
  ExpressionClass,
  ExpressionType,
  type CaseCheck,
  type CaseExpression,
  type CastExpression,
  type FunctionExpression,
  type JsonPathSegment,
  type Literal,
  type ParameterExpression,
  type ParsedExpression,
} from "./types.js";

export function parsePrefix(cursor: Cursor): ParsedExpression {
  const token = cursor.peek();
  switch (token.kind) {
    case "integer":
    case "real": {
      return parseNumber(cursor);
    }
    case "string": {
      cursor.next();
      return constantNode({ type: "TEXT", value: token.value });
    }
    case "blob": {
      cursor.next();
      return constantNode({ type: "BLOB", value: hexToBytes(token.value) });
    }
    case "parameter": {
      return parseParameter(cursor);
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

// --- Literals and parameters -------------------------------------------------------

/** SQLite reads at most 16 hex digits after any leading zeros: 64 bits. */
const MAX_HEX_DIGITS = 16;

/** A number, with the sign written before it if any: `-1` is one literal. */
function parseNumber(cursor: Cursor, sign?: Token): ParsedExpression {
  const token = cursor.next();
  const isReal = token.kind === "real";
  const magnitude = isReal
    ? Number(token.value)
    : integerValue(cursor, token, sign);
  const value = sign?.value === "-" ? negate(magnitude) : magnitude;
  return constantNode({ type: isReal ? "REAL" : "INTEGER", value });
}

/** `0 - value` rather than `-value`, so that `-0` is 0: no value is -0. */
function negate(value: number): number {
  return 0 - value;
}

/** An integer literal's value, which must be a safe integer (semantics.md). */
function integerValue(cursor: Cursor, token: Token, sign?: Token): number {
  const value = /^0x/i.test(token.text)
    ? hexValue(cursor, token)
    : Number(token.text);
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

/** A hex literal as SQLite reads it, 64-bit two's complement: 0xFF…FF is -1. */
function hexValue(cursor: Cursor, token: Token): number {
  const digits = token.text.slice(2).replace(/^0+/, "");
  if (digits.length > MAX_HEX_DIGITS) {
    cursor.failAt(
      token,
      `hex literal ${token.text} has more than ${String(MAX_HEX_DIGITS)} digits`,
    );
  }
  return Number(BigInt.asIntN(64, BigInt(token.text)));
}

function parseParameter(cursor: Cursor): ParameterExpression {
  const index = cursor.parameterIndex(cursor.next());
  return { expressionClass: ExpressionClass.PARAMETER, alias: null, index };
}

// --- Keywords and symbols ----------------------------------------------------

type PrefixParser = (cursor: Cursor) => ParsedExpression;

const KEYWORD_PREFIXES: ReadonlyMap<Keyword, PrefixParser> = new Map<
  Keyword,
  PrefixParser
>([
  ["NULL", (cursor) => keywordLiteral(cursor, { type: "NULL", value: null })],
  [
    "TRUE",
    (cursor) => keywordLiteral(cursor, { type: "BOOLEAN", value: true }),
  ],
  [
    "FALSE",
    (cursor) => keywordLiteral(cursor, { type: "BOOLEAN", value: false }),
  ],
  ["NOT", parseNot],
  ["EXISTS", parseExists],
  ["CASE", parseCase],
  ["CAST", parseCast],
  ["CURRENT_DATE", unsupportedPrefix("CURRENT_DATE")],
  ["CURRENT_TIME", unsupportedPrefix("CURRENT_TIME")],
  ["CURRENT_TIMESTAMP", unsupportedPrefix("CURRENT_TIMESTAMP")],
]);

const SYMBOL_PREFIXES: ReadonlyMap<SymbolText, PrefixParser> = new Map<
  SymbolText,
  PrefixParser
>([
  ["(", parseParenthesized],
  ["-", parseSigned],
  ["+", parseSigned],
  ["*", misplacedStar],
  ["~", unsupportedPrefix("the ~ operator")],
  ["[", unsupportedPrefix("a name in brackets")],
]);

function parseKeyword(cursor: Cursor, keyword: Keyword): ParsedExpression {
  const parse = KEYWORD_PREFIXES.get(keyword);
  if (parse !== undefined) return parse(cursor);
  return cursor.isName() ? parseName(cursor) : cursor.fail("an expression");
}

function parseSymbol(cursor: Cursor, symbol: SymbolText): ParsedExpression {
  const parse = SYMBOL_PREFIXES.get(symbol);
  return parse === undefined ? cursor.fail("an expression") : parse(cursor);
}

function keywordLiteral(cursor: Cursor, literal: Literal): ParsedExpression {
  cursor.next();
  return constantNode(literal);
}

function unsupportedPrefix(feature: string): PrefixParser {
  return () => {
    throw new NotSupportedError(feature);
  };
}

function misplacedStar(cursor: Cursor): never {
  return cursor.failAt(
    cursor.peek(),
    "'*' is allowed only as a select list item and in count(*)",
  );
}

/** `-x`, `+x`; a sign before a number is part of the literal. */
function parseSigned(cursor: Cursor): ParsedExpression {
  const sign = cursor.next();
  const kind = cursor.peek().kind;
  const isNumber = kind === "integer" || kind === "real";
  if (isNumber) return parseNumber(cursor, sign);
  const operand = cursor.expression(power.unary);
  if (sign.value === "+") return operand;
  return operatorNode(ExpressionType.OPERATOR_NEGATE, [operand]);
}

/** `NOT x`; `NOT EXISTS (…)` is one node. */
function parseNot(cursor: Cursor): ParsedExpression {
  cursor.next();
  const operand = cursor.expression(power.not);
  const isExists =
    operand.expressionClass === ExpressionClass.SUBQUERY &&
    operand.subqueryType === "EXISTS";
  return isExists
    ? subqueryNode("NOT_EXISTS", operand.subquery)
    : notNode(operand);
}

function parseExists(cursor: Cursor): ParsedExpression {
  cursor.next();
  return subqueryNode("EXISTS", cursor.subquery());
}

/** `( expression )` or a scalar subquery `( SELECT … )`. */
function parseParenthesized(cursor: Cursor): ParsedExpression {
  if (cursor.startsQuery(1)) return subqueryNode("SCALAR", cursor.subquery());
  cursor.next();
  const expression = cursor.expression();
  cursor.rejectSymbol(",", "a row value");
  cursor.expectSymbol(")");
  return expression;
}

// --- CASE and CAST -------------------------------------------------------------

function parseCase(cursor: Cursor): CaseExpression {
  cursor.next();
  const operand = cursor.isKeyword("WHEN") ? null : cursor.expression();
  const caseChecks = [parseCaseCheck(cursor)];
  while (cursor.isKeyword("WHEN")) caseChecks.push(parseCaseCheck(cursor));
  const elseExpression = cursor.expressionAfter("ELSE");
  cursor.expectKeyword("END");
  return {
    expressionClass: ExpressionClass.CASE,
    alias: null,
    operand,
    caseChecks,
    elseExpression,
  };
}

/** `WHEN … THEN …` */
function parseCaseCheck(cursor: Cursor): CaseCheck {
  cursor.expectKeyword("WHEN");
  const whenExpression = cursor.expression();
  cursor.expectKeyword("THEN");
  return { whenExpression, thenExpression: cursor.expression() };
}

function parseCast(cursor: Cursor): CastExpression {
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
    columnNames.push(cursor.nameAfterDot("a name after '.'"));
  }
  return {
    expressionClass: ExpressionClass.COLUMN_REF,
    alias: null,
    names: columnNames,
    path: parseJsonPath(cursor),
  };
}

/** `[0].field[1]…` after a column's names; a path starts with a bracket. */
function parseJsonPath(cursor: Cursor): JsonPathSegment[] {
  if (!cursor.isSymbol("[")) return [];
  const segments: JsonPathSegment[] = [];
  let segment = parsePathSegment(cursor);
  while (segment !== undefined) {
    segments.push(segment);
    segment = parsePathSegment(cursor);
  }
  return segments;
}

/** `.field` or `[index]`, or undefined at the end of the path. */
function parsePathSegment(cursor: Cursor): JsonPathSegment | undefined {
  if (cursor.matchSymbol(".")) {
    return { type: "field", name: cursor.nameAfterDot("a field name") };
  }
  if (!cursor.matchSymbol("[")) return undefined;
  if (cursor.peek().kind !== "integer") cursor.fail("an array index");
  const value = integerValue(cursor, cursor.next());
  cursor.expectSymbol("]");
  return { type: "index", value };
}

function parseCall(cursor: Cursor, name: string): FunctionExpression {
  const functionName = name.toLowerCase();
  cursor.expectSymbol("(");
  const { children, isStar, isDistinct } = parseArguments(cursor, functionName);
  cursor.rejectKeyword("ORDER", "ORDER BY in a function call");
  cursor.expectSymbol(")");
  cursor.rejectKeyword("OVER", "OVER (window functions)");
  cursor.rejectKeyword("FILTER", "FILTER");
  return {
    expressionClass: ExpressionClass.FUNCTION,
    alias: null,
    functionName,
    children,
    isDistinct,
    isStar,
  };
}

interface Arguments {
  readonly children: readonly ParsedExpression[];
  readonly isStar: boolean;
  readonly isDistinct: boolean;
}

/** Between a call's parentheses: `*` (count only), nothing, or expressions. */
function parseArguments(cursor: Cursor, functionName: string): Arguments {
  const isCountStar = functionName === "count" && cursor.isSymbol("*");
  if (isCountStar) {
    cursor.next();
    return { children: [], isStar: true, isDistinct: false };
  }
  if (cursor.matchKeyword("DISTINCT")) return parseArgumentList(cursor, true);
  // ALL is the default, as in SELECT ALL; arguments must follow it.
  if (cursor.matchKeyword("ALL")) return parseArgumentList(cursor, false);
  if (cursor.isSymbol(")")) {
    return { children: [], isStar: false, isDistinct: false };
  }
  return parseArgumentList(cursor, false);
}

function parseArgumentList(cursor: Cursor, isDistinct: boolean): Arguments {
  const children = cursor.list(() => cursor.expression());
  return { children, isStar: false, isDistinct };
}
