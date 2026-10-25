// ---------------------------------------------------------------------------
// Expressions, by precedence climbing (a Pratt parser). Each infix operator
// is a rule with a binding power from precedence.ts; one loop applies them,
// so a chain like `a + b + c` is parsed iteratively, and the right side of
// every operator is parsed at the level just above its own. Prefix forms
// live in primary.ts.
// ---------------------------------------------------------------------------

import { NotSupportedError } from "../../errors.js";
import type { Cursor } from "./cursor.js";
import type { Keyword } from "./keywords.js";
import { comparisonNode, notNode, operatorNode } from "./nodes.js";
import { above, power } from "./precedence.js";
import { parsePrefix, parseSubquery, startsQuery } from "./primary.js";
import type { SymbolText } from "./tokens.js";
import {
  ExpressionClass,
  ExpressionType,
  type ConjunctionExpression,
  type ParsedExpression,
  type SubqueryExpression,
} from "./types.js";

/** An expression whose operators all bind at least as tightly as `minimum`. */
export function parseExpression(
  cursor: Cursor,
  minimum: number,
): ParsedExpression {
  cursor.descend();
  let expression = parsePrefix(cursor);
  let levels = 1;
  for (
    let rule = infixRule(cursor);
    rule !== undefined && rule.power >= minimum;
    rule = infixRule(cursor)
  ) {
    expression = rule.parse(cursor, expression);
    // A chain of operators makes a tree as deep as a nesting would.
    cursor.descend();
    levels++;
  }
  cursor.ascend(levels);
  return expression;
}

interface InfixRule {
  readonly power: number;
  /** Parses the operator, at the cursor, and its right side. */
  readonly parse: (cursor: Cursor, left: ParsedExpression) => ParsedExpression;
}

function infixRule(cursor: Cursor): InfixRule | undefined {
  const token = cursor.peek();
  if (token.kind === "symbol") return SYMBOL_RULES.get(token.value);
  if (token.kind !== "keyword") return undefined;
  if (token.value !== "NOT") return KEYWORD_RULES.get(token.value);
  const next = cursor.peek(1);
  return next.kind === "keyword" ? NOT_RULES.get(next.value) : undefined;
}

// --- Binary operators ----------------------------------------------------------

function binary(type: ExpressionType, level: number): InfixRule {
  return {
    power: level,
    parse(cursor, left) {
      cursor.next();
      return operatorNode(type, [left, cursor.expression(above(level))]);
    },
  };
}

/** `a = b`, or a quantified comparison `a = ANY (SELECT …)`. */
function comparison(type: ExpressionType, level: number): InfixRule {
  return {
    power: level,
    parse(cursor, left) {
      cursor.next();
      const quantifier = quantifierAt(cursor);
      if (quantifier === undefined) {
        return comparisonNode(type, left, cursor.expression(above(level)));
      }
      cursor.next();
      return quantified(left, type, quantifier, parseSubquery(cursor));
    },
  };
}

/** ALL, ANY or SOME before a parenthesis. ANY and SOME are not keywords. */
function quantifierAt(cursor: Cursor): "ANY" | "ALL" | undefined {
  if (!cursor.isSymbol("(", 1)) return undefined;
  if (cursor.isKeyword("ALL")) return "ALL";
  const token = cursor.peek();
  const isAny =
    token.kind === "identifier" && /^(?:any|some)$/i.test(token.value);
  return isAny ? "ANY" : undefined;
}

function quantified(
  child: ParsedExpression,
  comparisonType: ExpressionType,
  subqueryType: "ANY" | "ALL",
  subquery: SubqueryExpression["subquery"],
): SubqueryExpression {
  return {
    expressionClass: ExpressionClass.SUBQUERY,
    alias: null,
    subqueryType,
    subquery,
    child,
    comparisonType,
  };
}

/** `a AND b AND c` as one node with three children. */
function conjunction(
  keyword: "AND" | "OR",
  type: ExpressionType,
  level: number,
): InfixRule {
  return {
    power: level,
    parse(cursor, left) {
      const children = [left];
      while (cursor.matchKeyword(keyword)) {
        children.push(cursor.expression(above(level)));
      }
      const node: ConjunctionExpression = {
        expressionClass: ExpressionClass.CONJUNCTION,
        alias: null,
        type,
        children,
      };
      return node;
    },
  };
}

function unsupported(feature: string, level: number): InfixRule {
  return {
    power: level,
    parse() {
      throw new NotSupportedError(feature);
    },
  };
}

// --- IS, IN, LIKE, BETWEEN and the NULL tests ------------------------------------

/** Skips the operator's words: `IN`, or `NOT` and `IN`. */
function skipOperator(cursor: Cursor, isNegated: boolean): void {
  if (isNegated) cursor.next();
  cursor.next();
}

/** `a IS [NOT] NULL`; IS with any other value is not supported. */
function parseIs(cursor: Cursor, left: ParsedExpression): ParsedExpression {
  cursor.next();
  const isNegated = cursor.matchKeyword("NOT");
  if (cursor.isKeyword("DISTINCT")) {
    throw new NotSupportedError("IS [NOT] DISTINCT FROM");
  }
  const right = cursor.expression(above(power.equality));
  if (
    right.expressionClass !== ExpressionClass.CONSTANT ||
    right.type !== "NULL"
  ) {
    throw new NotSupportedError("IS with a value other than NULL");
  }
  return nullTest(left, isNegated);
}

function nullTest(
  left: ParsedExpression,
  isNegated: boolean,
): ParsedExpression {
  const type = isNegated
    ? ExpressionType.OPERATOR_IS_NOT_NULL
    : ExpressionType.OPERATOR_IS_NULL;
  return operatorNode(type, [left]);
}

/** `a ISNULL`, `a NOTNULL`, and `a NOT NULL` (two words). */
function postfixNullTest(words: number, isNegated: boolean): InfixRule {
  return {
    power: power.equality,
    parse(cursor, left) {
      for (let word = 0; word < words; word++) cursor.next();
      return nullTest(left, isNegated);
    },
  };
}

/** `a [NOT] IN (list)` or `a [NOT] IN (SELECT …)`. */
function inRule(isNegated: boolean): InfixRule {
  return {
    power: power.equality,
    parse(cursor, left) {
      skipOperator(cursor, isNegated);
      if (cursor.isSymbol("(") && cursor.isSymbol(")", 1)) {
        throw new NotSupportedError("an empty IN list");
      }
      if (cursor.isName()) throw new NotSupportedError("IN with a table name");
      if (cursor.isSymbol("(") && startsQuery(cursor, 1)) {
        const test = quantified(
          left,
          ExpressionType.COMPARE_EQUAL,
          "ANY",
          parseSubquery(cursor),
        );
        return isNegated ? notNode(test) : test;
      }
      cursor.expectSymbol("(");
      const values = cursor.list(() => cursor.expression());
      cursor.expectSymbol(")");
      const type = isNegated
        ? ExpressionType.OPERATOR_NOT_IN
        : ExpressionType.OPERATOR_IN;
      return operatorNode(type, [left, ...values]);
    },
  };
}

/** `a [NOT] LIKE pattern`; ESCAPE is not supported. */
function likeRule(isNegated: boolean): InfixRule {
  return {
    power: power.equality,
    parse(cursor, left) {
      skipOperator(cursor, isNegated);
      const pattern = cursor.expression(above(power.equality));
      if (cursor.isKeyword("ESCAPE")) {
        throw new NotSupportedError("LIKE … ESCAPE");
      }
      const type = isNegated
        ? ExpressionType.COMPARE_NOT_LIKE
        : ExpressionType.COMPARE_LIKE;
      return comparisonNode(type, left, pattern);
    },
  };
}

/** `a [NOT] BETWEEN lower AND upper`. */
function betweenRule(isNegated: boolean): InfixRule {
  return {
    power: power.equality,
    parse(cursor, left) {
      skipOperator(cursor, isNegated);
      // As in SQLite, the lower bound may hold `=`, IS, IN, LIKE, BETWEEN.
      const lower = cursor.expression(power.equality);
      cursor.expectKeyword("AND");
      const upper = cursor.expression(above(power.equality));
      const between: ParsedExpression = {
        expressionClass: ExpressionClass.BETWEEN,
        alias: null,
        input: left,
        lower,
        upper,
      };
      return isNegated ? notNode(between) : between;
    },
  };
}

// --- The tables ----------------------------------------------------------------

const SYMBOL_RULES: ReadonlyMap<SymbolText, InfixRule> = new Map<
  SymbolText,
  InfixRule
>([
  ["=", comparison(ExpressionType.COMPARE_EQUAL, power.equality)],
  ["==", comparison(ExpressionType.COMPARE_EQUAL, power.equality)],
  ["!=", comparison(ExpressionType.COMPARE_NOTEQUAL, power.equality)],
  ["<>", comparison(ExpressionType.COMPARE_NOTEQUAL, power.equality)],
  ["<", comparison(ExpressionType.COMPARE_LESSTHAN, power.relational)],
  [
    "<=",
    comparison(ExpressionType.COMPARE_LESSTHANOREQUALTO, power.relational),
  ],
  [">", comparison(ExpressionType.COMPARE_GREATERTHAN, power.relational)],
  [
    ">=",
    comparison(ExpressionType.COMPARE_GREATERTHANOREQUALTO, power.relational),
  ],
  ["&", unsupported("the & operator", power.bitwise)],
  ["|", unsupported("the | operator", power.bitwise)],
  ["<<", unsupported("the << operator", power.bitwise)],
  [">>", unsupported("the >> operator", power.bitwise)],
  ["+", binary(ExpressionType.OPERATOR_ADD, power.additive)],
  ["-", binary(ExpressionType.OPERATOR_SUBTRACT, power.additive)],
  ["*", binary(ExpressionType.OPERATOR_MULTIPLY, power.multiplicative)],
  ["/", binary(ExpressionType.OPERATOR_DIVIDE, power.multiplicative)],
  ["%", binary(ExpressionType.OPERATOR_MOD, power.multiplicative)],
  ["||", binary(ExpressionType.OPERATOR_CONCAT, power.concatenation)],
  ["->", unsupported("the -> operator", power.concatenation)],
  ["->>", unsupported("the ->> operator", power.concatenation)],
]);

const KEYWORD_RULES: ReadonlyMap<Keyword, InfixRule> = new Map<
  Keyword,
  InfixRule
>([
  ["OR", conjunction("OR", ExpressionType.CONJUNCTION_OR, power.or)],
  ["AND", conjunction("AND", ExpressionType.CONJUNCTION_AND, power.and)],
  ["IS", { power: power.equality, parse: parseIs }],
  ["IN", inRule(false)],
  ["LIKE", likeRule(false)],
  ["BETWEEN", betweenRule(false)],
  ["ISNULL", postfixNullTest(1, false)],
  ["NOTNULL", postfixNullTest(1, true)],
  ["GLOB", unsupported("GLOB", power.equality)],
  ["REGEXP", unsupported("REGEXP", power.equality)],
  ["MATCH", unsupported("MATCH", power.equality)],
  ["COLLATE", unsupported("COLLATE", power.collation)],
]);

/** Operators spelled with NOT before them: `NOT IN`, `NOT NULL`, … */
const NOT_RULES: ReadonlyMap<Keyword, InfixRule> = new Map<Keyword, InfixRule>([
  ["IN", inRule(true)],
  ["LIKE", likeRule(true)],
  ["BETWEEN", betweenRule(true)],
  ["NULL", postfixNullTest(2, true)],
  ["GLOB", unsupported("GLOB", power.equality)],
  ["REGEXP", unsupported("REGEXP", power.equality)],
  ["MATCH", unsupported("MATCH", power.equality)],
]);
