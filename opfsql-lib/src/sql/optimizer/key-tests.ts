import { comparisonConversion } from "../../expressions/index.js";
import type {
  Between,
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
// operand, its subject, with seek values — constants other than NULL, and
// parameters (docs/planning.md, rule 5: a parameter bounds a range, it never
// picks the index; nothing here looks at a parameter's value, or at whether
// a value is a parameter, except to keep its condition in the residual).
//
//   subject = v                        equal
//   subject IN (v, …)                  in
//   subject < v, <=, >, >=, BETWEEN    range
//
// A test fits a key part when the subject is what the part holds, of the
// part's type, and no value makes `=` convert the subject: a part of type
// ANY compared with a typed value is converted per row by affinity (ANY
// 5 = TEXT '5' is true), which an index of the stored values cannot follow.
// Seek values themselves are converted as `=` converts them, by the
// executor (docs/planning.md, Physical plans).
// ---------------------------------------------------------------------------

type Expression = physical.Expression;

/** A seek value: the same for every row of a run. */
export type SeekValue = Constant | Parameter;

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

/** `condition` as a key test, or null if it is not one. */
export function keyTestOf(condition: Expression): KeyTest | null {
  if (condition.kind === "comparison") return comparisonTest(condition);
  if (condition.kind === "between") return betweenTest(condition);
  if (condition.kind === "in-list") return inListTest(condition);
  return null;
}

function isSeekValue(expression: Expression): expression is SeekValue {
  if (expression.kind === "parameter") return true;
  return expression.kind === "constant" && expression.value !== null;
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

/** `v < x` is `x > v`: each side converts by its own type, so either order compares alike. */
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

/** The seek values a test compares with. */
function valuesOf(test: Test): SeekValue[] {
  switch (test.kind) {
    case "equal": {
      return [test.value];
    }
    case "in": {
      return [...test.values];
    }
    case "range": {
      const { lower, upper } = test.range;
      return [lower, upper].flatMap((bound) =>
        bound === null ? [] : [bound.value],
      );
    }
  }
}

/** True if the test can seek on `part` (see above). */
export function fitsPart(keyTest: KeyTest, part: KeyPart): boolean {
  const { subject } = keyTest;
  const isHeld =
    subject.type === part.type && sameExpression(subject, part.expression);
  if (!isHeld) return false;
  return valuesOf(keyTest.test).every((value) => isUnconverting(part, value));
}

/** True if comparing the part with `value` leaves the part's values as they are. */
function isUnconverting(part: KeyPart, value: SeekValue): boolean {
  return part.type !== "ANY" || value.type === "ANY";
}

/**
 * True if seeking with the test returns exactly the rows the condition
 * holds for, so the residual may leave it out (docs/planning.md, rule 1).
 * Only for constants: a parameter may be NULL at run time, where `=` holds
 * for nothing but a seek could still find NULL entries. And a range with
 * an upper bound starts at the index's first entry, where NULLs sort, so
 * it is exact only for a part that holds no NULL.
 */
export function isExact(keyTest: KeyTest, part: KeyPart): boolean {
  const values = valuesOf(keyTest.test);
  const isConstant = values.every((value) => value.kind === "constant");
  const { test } = keyTest;
  const hasUpper = test.kind === "range" && test.range.upper !== null;
  return isConstant && (!hasUpper || part.isNeverNull);
}

/**
 * True if every value can find a rowid: a parameter (the executor converts
 * it), or a constant that `=` with an INTEGER turns into a number. Any other
 * constant can never equal a rowid, and is left to the residual.
 */
export function seeksRowids(test: Test): boolean {
  return valuesOf(test).every((value) => {
    if (value.kind === "parameter") return true;
    const converted = comparisonConversion(value.type, "INTEGER")(value.value);
    return typeof converted === "number";
  });
}
