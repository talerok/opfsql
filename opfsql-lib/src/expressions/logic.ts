import type {
  Between,
  Comparison,
  Conjunction,
  IsNull,
  Not,
  physical,
} from "../sql/plan/index.js";
import { and, foldTruth, not, type Truth, truthOf } from "../values/index.js";
import { comparison } from "./comparison.js";
import { compileInList } from "./in-list.js";
import type { CompiledExpression, Compiler, Tuple } from "./scope.js";

// ---------------------------------------------------------------------------
// Conditions, three-valued as values defines them: comparisons give NULL
// for a NULL side, and AND, OR and NOT treat NULL as "unknown". An operand
// that is neither BOOLEAN nor NULL is a DATATYPE_MISMATCH (truthOf).
// ---------------------------------------------------------------------------

/** The expressions whose value is a truth: BOOLEAN or NULL. */
export type Condition = Extract<
  physical.Expression,
  {
    readonly kind:
      "comparison" | "conjunction" | "not" | "is-null" | "in-list" | "between";
  }
>;

export function compileCondition(
  expression: Condition,
  compiler: Compiler,
): CompiledExpression {
  switch (expression.kind) {
    case "comparison": {
      return compileComparison(expression, compiler);
    }
    case "conjunction": {
      return compileConjunction(expression, compiler);
    }
    case "not": {
      return compileNot(expression, compiler);
    }
    case "is-null": {
      return compileIsNull(expression, compiler);
    }
    case "in-list": {
      return compileInList(expression, compiler);
    }
    case "between": {
      return compileBetween(expression, compiler);
    }
  }
}

function compileComparison(
  expression: Comparison<physical.Plan>,
  compiler: Compiler,
): CompiledExpression {
  const test = comparison(expression.operator);
  const leftValue = compiler.compile(expression.left);
  const rightValue = compiler.compile(expression.right);
  return (row) => test(leftValue(row), rightValue(row));
}

function compileConjunction(
  expression: Conjunction<physical.Plan>,
  compiler: Compiler,
): CompiledExpression {
  const operands = expression.operands.map((operand) =>
    compiler.compile(operand),
  );
  const kind = expression.operator === "AND" ? "and" : "or";
  // foldTruth stops once the answer is decided, so the generator never
  // evaluates the remaining operands.
  function* truths(row: Tuple): Generator<Truth> {
    for (const operand of operands) yield truthOf(operand(row));
  }
  return (row) => foldTruth(kind, truths(row));
}

function compileNot(
  expression: Not<physical.Plan>,
  compiler: Compiler,
): CompiledExpression {
  const operand = compiler.compile(expression.operand);
  return (row) => not(truthOf(operand(row)));
}

/** IS NULL: never NULL itself. */
function compileIsNull(
  expression: IsNull<physical.Plan>,
  compiler: Compiler,
): CompiledExpression {
  const operand = compiler.compile(expression.operand);
  return (row) => operand(row) === null;
}

/** `x BETWEEN a AND b` is `x >= a AND x <= b`, with x evaluated once. */
function compileBetween(
  expression: Between<physical.Plan>,
  compiler: Compiler,
): CompiledExpression {
  const atLeast = comparison("GREATER_EQUAL");
  const atMost = comparison("LESS_EQUAL");
  const inputValue = compiler.compile(expression.input);
  const lowerValue = compiler.compile(expression.lower);
  const upperValue = compiler.compile(expression.upper);
  return (row) => {
    const value = inputValue(row);
    return and(atLeast(value, lowerValue(row)), atMost(value, upperValue(row)));
  };
}
