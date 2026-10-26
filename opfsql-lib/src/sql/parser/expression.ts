// ---------------------------------------------------------------------------
// Expressions, by precedence climbing (a Pratt parser). Each infix operator
// is a rule with a binding power from precedence.ts; one chain applies them,
// so `a + b + c` is parsed iteratively, and the right side of every operator
// is parsed at the level just above its own. Prefix forms live in primary.ts.
// ---------------------------------------------------------------------------

import { NotSupportedError } from "../../errors.js";
import type { Cursor } from "./cursor.js";
import type { Keyword } from "./keywords.js";
import {
  comparisonNode,
  notNode,
  operatorNode,
  quantifiedNode,
} from "./nodes.js";
import { above, power } from "../precedence.js";
import { parsePrefix } from "./primary.js";
import type { SymbolText } from "./tokens.js";
import {
  ExpressionClass,
  ExpressionType,
  type ComparisonType,
  type ConjunctionType,
  type OperatorType,
  type ParsedExpression,
} from "./types.js";

/** An expression whose operators all bind at least as tightly as `minimum`. */
export function parseExpression(
  cursor: Cursor,
  minimum: number,
): ParsedExpression {
  cursor.descend();
  const expression = cursor.chain(
    parsePrefix(cursor),
    () => infixRuleAt(cursor, minimum),
    (left, rule) => rule.parse(cursor, left),
  );
  cursor.ascend();
  return expression;
}

interface InfixRule {
  readonly power: number;
  /** Parses the operator, starting at its first word, and its right side. */
  readonly parse: (cursor: Cursor, left: ParsedExpression) => ParsedExpression;
}

/** The operator at the cursor, if it binds at least as tightly as `minimum`. */
function infixRuleAt(cursor: Cursor, minimum: number): InfixRule | undefined {
  const rule = infixRule(cursor);
  return rule !== undefined && rule.power >= minimum ? rule : undefined;
}

function infixRule(cursor: Cursor): InfixRule | undefined {
  const token = cursor.peek();
  if (token.kind === "symbol") return SYMBOL_RULES.get(token.value);
  if (cursor.isKeyword("NOT")) return cursor.lookupKeyword(NOT_RULES, 1);
  return cursor.lookupKeyword(KEYWORD_RULES);
}

// --- Binary operators ----------------------------------------------------------

function binary(type: OperatorType, level: number): InfixRule {
  return {
    power: level,
    parse(cursor, left) {
      cursor.next();
      return operatorNode(type, [left, cursor.expression(above(level))]);
    },
  };
}

/** `a = b`, or a quantified comparison `a = ANY (SELECT …)`. */
function comparison(type: ComparisonType, level: number): InfixRule {
  return {
    power: level,
    parse(cursor, left) {
      cursor.next();
      const quantifier = quantifierAt(cursor);
      if (quantifier === undefined) {
        return comparisonNode(type, left, cursor.expression(above(level)));
      }
      cursor.next();
      return quantifiedNode(left, type, quantifier, cursor.subquery());
    },
  };
}

/** ALL, ANY or SOME before a parenthesis. ANY and SOME are not keywords. */
function quantifierAt(cursor: Cursor): "ANY" | "ALL" | undefined {
  if (!cursor.isSymbol("(", 1)) return undefined;
  if (cursor.isKeyword("ALL")) return "ALL";
  const isAny = cursor.isWord("ANY") || cursor.isWord("SOME");
  return isAny ? "ANY" : undefined;
}

/** `a AND b AND c` as one node with three children. */
function conjunction(
  keyword: "AND" | "OR",
  type: ConjunctionType,
  level: number,
): InfixRule {
  return {
    power: level,
    parse(cursor, left) {
      const children = [left];
      do {
        cursor.next();
        children.push(cursor.expression(above(level)));
      } while (cursor.isKeyword(keyword));
      return {
        expressionClass: ExpressionClass.CONJUNCTION,
        alias: null,
        type,
        children,
      };
    },
  };
}

/** `a ISNULL`, `a NOTNULL`, and NULL in `a NOT NULL`. */
function postfix(type: OperatorType): InfixRule {
  return {
    power: power.equality,
    parse(cursor, left) {
      cursor.next();
      return operatorNode(type, [left]);
    },
  };
}

function unsupportedInfix(feature: string, level: number): InfixRule {
  return {
    power: level,
    parse() {
      throw new NotSupportedError(feature);
    },
  };
}

// --- IS, IN, LIKE and BETWEEN ------------------------------------------------------

/** `a IS [NOT] NULL`; IS with any other value is not supported. */
const NULL_TEST_RULE: InfixRule = {
  power: power.equality,
  parse(cursor, left) {
    cursor.next();
    const isNegated = cursor.matchKeyword("NOT");
    cursor.rejectKeyword("DISTINCT", "IS [NOT] DISTINCT FROM");
    const right = cursor.expression(above(power.equality));
    const isNull =
      right.expressionClass === ExpressionClass.CONSTANT &&
      right.type === "NULL";
    if (!isNull) throw new NotSupportedError("IS with a value other than NULL");
    const type = isNegated
      ? ExpressionType.OPERATOR_IS_NOT_NULL
      : ExpressionType.OPERATOR_IS_NULL;
    return operatorNode(type, [left]);
  },
};

/** `a IN (list)`, the list maybe empty, or `a IN (SELECT …)`. */
const IN_RULE: InfixRule = {
  power: power.equality,
  parse(cursor, left) {
    cursor.next();
    cursor.rejectName("IN with a table name");
    if (cursor.startsQuery(1)) {
      const subquery = cursor.subquery();
      return quantifiedNode(
        left,
        ExpressionType.COMPARE_EQUAL,
        "ANY",
        subquery,
      );
    }
    cursor.expectSymbol("(");
    const isEmpty = cursor.isSymbol(")");
    const values = isEmpty ? [] : cursor.list(() => cursor.expression());
    cursor.expectSymbol(")");
    return operatorNode(ExpressionType.OPERATOR_IN, [left, ...values]);
  },
};

/** `a LIKE pattern`; ESCAPE is not supported. */
const LIKE_RULE: InfixRule = {
  power: power.equality,
  parse(cursor, left) {
    cursor.next();
    const pattern = cursor.expression(above(power.equality));
    cursor.rejectKeyword("ESCAPE", "LIKE … ESCAPE");
    return comparisonNode(ExpressionType.COMPARE_LIKE, left, pattern);
  },
};

/** `a BETWEEN lower AND upper`. */
const BETWEEN_RULE: InfixRule = {
  power: power.equality,
  parse(cursor, left) {
    cursor.next();
    // As in SQLite, the lower bound may hold `=`, IS, IN, LIKE, BETWEEN.
    const lower = cursor.expression(power.equality);
    cursor.expectKeyword("AND");
    const upper = cursor.expression(above(power.equality));
    return {
      expressionClass: ExpressionClass.BETWEEN,
      alias: null,
      input: left,
      lower,
      upper,
    };
  },
};

/** `a NOT <rule>`: NOT, then the rule's own words, then NOT around it all. */
function negated(rule: InfixRule): InfixRule {
  return {
    power: rule.power,
    parse(cursor, left) {
      cursor.next();
      return negation(rule.parse(cursor, left));
    },
  };
}

/** NOT around a node; NOT IN and IS NOT NULL are operators of their own. */
function negation(node: ParsedExpression): ParsedExpression {
  const isOperator = node.expressionClass === ExpressionClass.OPERATOR;
  if (isOperator && node.type === ExpressionType.OPERATOR_IN) {
    return operatorNode(ExpressionType.OPERATOR_NOT_IN, node.children);
  }
  if (isOperator && node.type === ExpressionType.OPERATOR_IS_NULL) {
    return operatorNode(ExpressionType.OPERATOR_IS_NOT_NULL, node.children);
  }
  return notNode(node);
}

// --- The tables ----------------------------------------------------------------

const GLOB_RULE = unsupportedInfix("GLOB", power.equality);
const REGEXP_RULE = unsupportedInfix("REGEXP", power.equality);
const MATCH_RULE = unsupportedInfix("MATCH", power.equality);

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
  ["&", unsupportedInfix("the & operator", power.bitwise)],
  ["|", unsupportedInfix("the | operator", power.bitwise)],
  ["<<", unsupportedInfix("the << operator", power.bitwise)],
  [">>", unsupportedInfix("the >> operator", power.bitwise)],
  ["+", binary(ExpressionType.OPERATOR_ADD, power.additive)],
  ["-", binary(ExpressionType.OPERATOR_SUBTRACT, power.additive)],
  ["*", binary(ExpressionType.OPERATOR_MULTIPLY, power.multiplicative)],
  ["/", binary(ExpressionType.OPERATOR_DIVIDE, power.multiplicative)],
  ["%", binary(ExpressionType.OPERATOR_MOD, power.multiplicative)],
  ["||", binary(ExpressionType.OPERATOR_CONCAT, power.concatenation)],
  ["->", unsupportedInfix("the -> operator", power.concatenation)],
  ["->>", unsupportedInfix("the ->> operator", power.concatenation)],
]);

const KEYWORD_RULES: ReadonlyMap<Keyword, InfixRule> = new Map<
  Keyword,
  InfixRule
>([
  ["OR", conjunction("OR", ExpressionType.CONJUNCTION_OR, power.or)],
  ["AND", conjunction("AND", ExpressionType.CONJUNCTION_AND, power.and)],
  ["IS", NULL_TEST_RULE],
  ["IN", IN_RULE],
  ["LIKE", LIKE_RULE],
  ["BETWEEN", BETWEEN_RULE],
  ["ISNULL", postfix(ExpressionType.OPERATOR_IS_NULL)],
  ["NOTNULL", postfix(ExpressionType.OPERATOR_IS_NOT_NULL)],
  ["GLOB", GLOB_RULE],
  ["REGEXP", REGEXP_RULE],
  ["MATCH", MATCH_RULE],
  ["COLLATE", unsupportedInfix("COLLATE", power.collation)],
]);

/** Operators written with NOT before their own word: `NOT IN`, `NOT NULL`. */
const NOT_RULES: ReadonlyMap<Keyword, InfixRule> = new Map<Keyword, InfixRule>([
  ["IN", negated(IN_RULE)],
  ["LIKE", negated(LIKE_RULE)],
  ["BETWEEN", negated(BETWEEN_RULE)],
  ["NULL", negated(postfix(ExpressionType.OPERATOR_IS_NULL))],
  ["GLOB", GLOB_RULE],
  ["REGEXP", REGEXP_RULE],
  ["MATCH", MATCH_RULE],
]);
