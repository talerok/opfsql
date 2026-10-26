import { InternalError } from "../../errors.js";
import { resolveCall, scalarFunction } from "../../functions/index.js";
import {
  ExpressionType,
  type BetweenExpression,
  type ComparisonExpression,
  type ComparisonType,
  type ConjunctionExpression,
  type OperatorExpression,
  type ParsedExpression,
} from "../parser/index.js";
import type {
  ArithmeticOperator,
  ComparisonOperator,
  logical,
} from "../plan/index.js";
import { comparedWith } from "./affinity.js";
import { bindCondition, type Clause } from "./clause.js";
import {
  arithmeticType,
  checkCondition,
  checkComparable,
  concatenationType,
  negationType,
} from "./type-rules.js";

// ---------------------------------------------------------------------------
// Comparisons, logic and arithmetic. Negation is always a `not` node: NOT
// IN, IS NOT NULL and NOT LIKE bind as NOT around IN, IS NULL and LIKE.
// ---------------------------------------------------------------------------

type Expression = logical.Expression;

export function bindComparison(
  expression: ComparisonExpression,
  clause: Clause,
): Expression {
  const left = clause.bind(expression.left);
  const right = clause.bind(expression.right);
  if (expression.type === ExpressionType.COMPARE_LIKE) {
    return likeCall(left, right);
  }
  checkComparable(left.type, right.type);
  return {
    kind: "comparison",
    operator: comparisonOperator(expression.type),
    left: comparedWith(left, [right.type]),
    right: comparedWith(right, [left.type]),
    type: "BOOLEAN",
  };
}

/** `a AND b AND c` as one node; an AND inside an AND is flattened into it. */
export function bindConjunction(
  expression: ConjunctionExpression,
  clause: Clause,
): Expression {
  const isAnd = expression.type === ExpressionType.CONJUNCTION_AND;
  const operator = isAnd ? "AND" : "OR";
  const operands = expression.children.flatMap((child) => {
    const operand = bindCondition(child, clause, `an operand of ${operator}`);
    const isSame =
      operand.kind === "conjunction" && operand.operator === operator;
    return isSame ? operand.operands : [operand];
  });
  return { kind: "conjunction", operator, operands, type: "BOOLEAN" };
}

export function bindOperator(
  expression: OperatorExpression,
  clause: Clause,
): Expression {
  const [first, ...rest] = expression.children;
  const subject = clause.bind(required(first));
  switch (expression.type) {
    case ExpressionType.OPERATOR_NOT: {
      return not(conditionOf(subject));
    }
    case ExpressionType.OPERATOR_IS_NULL: {
      return nullTest(subject);
    }
    case ExpressionType.OPERATOR_IS_NOT_NULL: {
      return not(nullTest(subject));
    }
    case ExpressionType.OPERATOR_IN: {
      return inList(subject, rest, clause);
    }
    case ExpressionType.OPERATOR_NOT_IN: {
      return not(inList(subject, rest, clause));
    }
    case ExpressionType.OPERATOR_NEGATE: {
      const type = negationType(subject.type);
      return { kind: "negate", operand: subject, type };
    }
    case ExpressionType.OPERATOR_CONCAT: {
      const right = clause.bind(required(rest[0]));
      const type = concatenationType(subject.type, right.type);
      return { kind: "concatenate", left: subject, right, type };
    }
    case ExpressionType.OPERATOR_ADD:
    case ExpressionType.OPERATOR_SUBTRACT:
    case ExpressionType.OPERATOR_MULTIPLY:
    case ExpressionType.OPERATOR_DIVIDE:
    case ExpressionType.OPERATOR_MOD: {
      const right = clause.bind(required(rest[0]));
      const operator = arithmeticOperator(expression.type);
      const type = arithmeticType(subject.type, right.type);
      return { kind: "arithmetic", operator, left: subject, right, type };
    }
  }
}

/** `x BETWEEN a AND b`: x is compared with each bound as `>=` and `<=` would. */
export function bindBetween(
  expression: BetweenExpression,
  clause: Clause,
): Expression {
  const input = clause.bind(expression.input);
  const lower = clause.bind(expression.lower);
  const upper = clause.bind(expression.upper);
  checkComparable(input.type, lower.type);
  checkComparable(input.type, upper.type);
  return {
    kind: "between",
    input: comparedWith(input, [lower.type, upper.type]),
    lower: comparedWith(lower, [input.type]),
    upper: comparedWith(upper, [input.type]),
    type: "BOOLEAN",
  };
}

export function not(operand: Expression): Expression {
  return { kind: "not", operand, type: "BOOLEAN" };
}

export function comparisonOperator(type: ComparisonType): ComparisonOperator {
  switch (type) {
    case ExpressionType.COMPARE_EQUAL: {
      return "EQUAL";
    }
    case ExpressionType.COMPARE_NOTEQUAL: {
      return "NOT_EQUAL";
    }
    case ExpressionType.COMPARE_LESSTHAN: {
      return "LESS";
    }
    case ExpressionType.COMPARE_LESSTHANOREQUALTO: {
      return "LESS_EQUAL";
    }
    case ExpressionType.COMPARE_GREATERTHAN: {
      return "GREATER";
    }
    case ExpressionType.COMPARE_GREATERTHANOREQUALTO: {
      return "GREATER_EQUAL";
    }
  }
}

function arithmeticOperator(
  type:
    | ExpressionType.OPERATOR_ADD
    | ExpressionType.OPERATOR_SUBTRACT
    | ExpressionType.OPERATOR_MULTIPLY
    | ExpressionType.OPERATOR_DIVIDE
    | ExpressionType.OPERATOR_MOD,
): ArithmeticOperator {
  switch (type) {
    case ExpressionType.OPERATOR_ADD: {
      return "ADD";
    }
    case ExpressionType.OPERATOR_SUBTRACT: {
      return "SUBTRACT";
    }
    case ExpressionType.OPERATOR_MULTIPLY: {
      return "MULTIPLY";
    }
    case ExpressionType.OPERATOR_DIVIDE: {
      return "DIVIDE";
    }
    case ExpressionType.OPERATOR_MOD: {
      return "MOD";
    }
  }
}

/** `text LIKE pattern` is SQLite's like(pattern, text). */
function likeCall(text: Expression, pattern: Expression): Expression {
  const definition = scalarFunction("like");
  const argumentList = [pattern, text];
  const type = resolveCall(
    definition,
    argumentList.map((argument) => argument.type),
  );
  return {
    kind: "function",
    name: "like",
    function: definition,
    arguments: argumentList,
    type,
  };
}

function conditionOf(operand: Expression): Expression {
  checkCondition(operand.type, "the operand of NOT");
  return operand;
}

function nullTest(operand: Expression): Expression {
  return { kind: "is-null", operand, type: "BOOLEAN" };
}

/** `x IN (a, b)`: x is compared with each item as `=` would. */
function inList(
  needle: Expression,
  items: readonly ParsedExpression[],
  clause: Clause,
): Expression {
  const list = items.map((item) => clause.bind(item));
  for (const item of list) checkComparable(needle.type, item.type);
  return {
    kind: "in-list",
    needle: comparedWith(
      needle,
      list.map((item) => item.type),
    ),
    list: list.map((item) => comparedWith(item, [needle.type])),
    type: "BOOLEAN",
  };
}

/** An operand the parser always builds; a tree without it is a bug. */
function required(expression: ParsedExpression | undefined): ParsedExpression {
  if (expression === undefined) {
    throw new InternalError("an operator is missing an operand");
  }
  return expression;
}
