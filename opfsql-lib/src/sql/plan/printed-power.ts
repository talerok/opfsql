import { above, power } from "../precedence.js";
import type {
  ArithmeticOperator,
  ComparisonOperator,
  Expression,
} from "./expression.js";

// ---------------------------------------------------------------------------
// How tightly a printed bound expression holds together, in the binding
// powers the parser parses by (sql/precedence.ts), so the printer puts
// parentheses exactly where reading the text back needs them.
// ---------------------------------------------------------------------------

/** What binds tighter than every operator: names, literals, calls, `( … )`. */
export const PRIMARY = above(power.unary);

export function powerOf<Plan>(expression: Expression<Plan>): number {
  switch (expression.kind) {
    case "column":
    case "constant":
    case "parameter":
    case "function":
    case "case":
    case "cast":
    case "json-access":
    case "scalar-subquery":
    case "exists": {
      return PRIMARY;
    }
    case "conjunction": {
      return expression.operator === "AND" ? power.and : power.or;
    }
    case "not": {
      return negationPower(expression.operand);
    }
    case "comparison":
    case "quantified-comparison": {
      return comparisonPower(expression.operator);
    }
    case "is-null":
    case "in-list":
    case "between": {
      return power.equality;
    }
    case "arithmetic": {
      return arithmeticPower(expression.operator);
    }
    case "concatenate": {
      return power.concatenation;
    }
    case "negate": {
      return power.unary;
    }
  }
}

/** `=` and `<>` bind more loosely than `<`, `<=`, `>` and `>=`, as in SQLite. */
export function comparisonPower(operator: ComparisonOperator): number {
  const isEquality = operator === "EQUAL" || operator === "NOT_EQUAL";
  return isEquality ? power.equality : power.relational;
}

function arithmeticPower(operator: ArithmeticOperator): number {
  const isAdditive = operator === "ADD" || operator === "SUBTRACT";
  return isAdditive ? power.additive : power.multiplicative;
}

/** The kinds a NOT over them prints as one infix operator with: `x NOT IN`, … */
const NEGATED_INFIX: ReadonlySet<string> = new Set([
  "in-list",
  "is-null",
  "between",
]);

/**
 * NOT over IN, IS NULL or BETWEEN prints as `x NOT IN …`, `x IS NOT NULL`,
 * `x NOT BETWEEN …`, which bind as the operators they negate; any other NOT
 * is the prefix NOT.
 */
function negationPower<Plan>(operand: Expression<Plan>): number {
  return NEGATED_INFIX.has(operand.kind) ? power.equality : power.not;
}
