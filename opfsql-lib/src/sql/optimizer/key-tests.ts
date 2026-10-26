import type {
  Between,
  ColumnReference,
  Comparison,
  ComparisonOperator,
  Constant,
  InList,
  Parameter,
  physical,
} from "../plan/index.js";
import type { KeyPart } from "./key-parts.js";
import { sameExpression } from "./same-expression.js";

// ---------------------------------------------------------------------------
// Conditions an access path can seek with: a conjunct that compares one
// operand, its subject, with seek values — constants, parameters and
// columns of enclosing queries, each fixed for one scan (docs/planning.md,
// Physical plans), so a correlated subquery seeks on its outer row. A
// column of the scan's own query changes from row to row: never a seek
// value.
//
//   subject = v                        equal
//   subject IN (v, …)                  in
//   subject < v, <=, >, >=, BETWEEN    range
//
// A test fits a key part when the subject is what the part holds. `=` and
// `<` compare values as they are (a parameter's affinity is part of the
// parameter), and an index orders its entries by the same total order, so
// a seek finds exactly the entries the comparison holds for — a NULL seek
// value none, as `=` with NULL never holds. Nothing here looks at a
// parameter's value (rule 5).
// ---------------------------------------------------------------------------

type Expression = physical.Expression;

/** A seek value: the same for every row of one scan. */
export type SeekValue = Constant | Parameter | ColumnReference;

export interface SeekBound {
  readonly value: SeekValue;
  readonly isInclusive: boolean;
}

/** A physical.Range whose bounds are seek values; null at an open end. */
export interface SeekRange {
  readonly lower: SeekBound | null;
  readonly upper: SeekBound | null;
}

export type Test =
  | { readonly kind: "equal"; readonly value: SeekValue }
  | { readonly kind: "in"; readonly values: readonly SeekValue[] }
  | { readonly kind: "range"; readonly range: SeekRange };

export interface KeyTest {
  /** The conjunct it was read from. */
  readonly condition: Expression;
  readonly subject: Expression;
  readonly test: Test;
}

/** A test that lists the values it seeks: `=` or IN. */
export type ListTest = KeyTest & {
  readonly test: Extract<Test, { readonly kind: "equal" | "in" }>;
};

export function isListTest(keyTest: KeyTest): keyTest is ListTest {
  return keyTest.test.kind !== "range";
}

/** `condition` as a key test, or null if it is not one. */
export function keyTestOf(condition: Expression): KeyTest | null {
  if (condition.kind === "comparison") return comparisonTest(condition);
  if (condition.kind === "between") return betweenTest(condition);
  if (condition.kind === "in-list") return inListTest(condition);
  return null;
}

function isSeekValue(expression: Expression): expression is SeekValue {
  if (expression.kind === "column") return expression.depth > 0;
  return expression.kind === "constant" || expression.kind === "parameter";
}

function comparisonTest(condition: Comparison<physical.Plan>): KeyTest | null {
  const { left, right, operator } = condition;
  if (isSeekValue(right) && !isSeekValue(left)) {
    return comparedWith(condition, left, operator, right);
  }
  if (isSeekValue(left) && !isSeekValue(right)) {
    return comparedWith(condition, right, mirrored(operator), left);
  }
  return null;
}

/** `subject <operator> value` as a test. */
function comparedWith(
  condition: Expression,
  subject: Expression,
  operator: ComparisonOperator,
  value: SeekValue,
): KeyTest | null {
  const test = testFor(operator, value);
  return test === null ? null : { condition, subject, test };
}

function testFor(operator: ComparisonOperator, value: SeekValue): Test | null {
  const inclusive = { value, isInclusive: true };
  const exclusive = { value, isInclusive: false };
  switch (operator) {
    case "EQUAL": {
      return { kind: "equal", value };
    }
    case "NOT_EQUAL": {
      return null;
    }
    case "LESS": {
      return { kind: "range", range: { lower: null, upper: exclusive } };
    }
    case "LESS_EQUAL": {
      return { kind: "range", range: { lower: null, upper: inclusive } };
    }
    case "GREATER": {
      return { kind: "range", range: { lower: exclusive, upper: null } };
    }
    case "GREATER_EQUAL": {
      return { kind: "range", range: { lower: inclusive, upper: null } };
    }
  }
}

/** `v < x` is `x > v`. */
function mirrored(operator: ComparisonOperator): ComparisonOperator {
  switch (operator) {
    case "EQUAL":
    case "NOT_EQUAL": {
      return operator;
    }
    case "LESS": {
      return "GREATER";
    }
    case "LESS_EQUAL": {
      return "GREATER_EQUAL";
    }
    case "GREATER": {
      return "LESS";
    }
    case "GREATER_EQUAL": {
      return "LESS_EQUAL";
    }
  }
}

function betweenTest(condition: Between<physical.Plan>): KeyTest | null {
  const { input, lower, upper } = condition;
  const isRange =
    isSeekValue(lower) && isSeekValue(upper) && !isSeekValue(input);
  if (!isRange) return null;
  const range = {
    lower: { value: lower, isInclusive: true },
    upper: { value: upper, isInclusive: true },
  };
  return { condition, subject: input, test: { kind: "range", range } };
}

function inListTest(condition: InList<physical.Plan>): KeyTest | null {
  const { needle, list } = condition;
  const values = list.filter((item) => isSeekValue(item));
  const isFixedList = values.length > 0 && values.length === list.length;
  if (!isFixedList || isSeekValue(needle)) return null;
  return { condition, subject: needle, test: { kind: "in", values } };
}

/** True if the test can seek on `part`: its subject is what the part holds. */
export function fitsPart(keyTest: KeyTest, part: KeyPart): boolean {
  return sameExpression(keyTest.subject, part.expression);
}

/**
 * True if a range finds exactly the entries its bounds hold for, so the
 * residual may leave out the tests it came from (docs/planning.md, rule 1).
 * NULL sorts before every value, so a range open below starts at the NULL
 * entries, where `<` never holds: exact only on a part that holds no NULL.
 * An `=` or IN seek is always exact.
 */
export function isExactRange(range: SeekRange, part: KeyPart): boolean {
  return range.lower !== null || part.isNeverNull;
}
