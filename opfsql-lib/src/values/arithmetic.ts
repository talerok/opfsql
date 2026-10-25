import { DatatypeMismatchError, IntegerOverflowError } from "../errors.js";
import { castValue, describeValue, numberFromText } from "./convert.js";
import type { Value } from "./value.js";

// ---------------------------------------------------------------------------
// Arithmetic and concatenation (docs/semantics.md, Arithmetic). NULL in,
// NULL out; INTEGER results must stay safe integers; dividing by zero is NULL.
// ---------------------------------------------------------------------------

export type ArithmeticOperator =
  "add" | "subtract" | "multiply" | "divide" | "remainder";

/**
 * How operands are treated: `integer` when both have the static type
 * INTEGER, `real` when one is REAL, `dynamic` when a static type is missing
 * (ANY, JSON, NULL) and the values decide: two safe integers are integers.
 */
export type NumericMode = "integer" | "real" | "dynamic";

type Operation = (left: number, right: number) => number | null;

/** The binary operator for `mode`, chosen once and applied per row. */
export function arithmetic(
  operator: ArithmeticOperator,
  mode: NumericMode,
): (left: Value, right: Value) => Value {
  const onIntegers = integerOperation(operator);
  const onReals = realOperation(operator);
  return (left, right) => {
    if (left === null || right === null) return null;
    const a = numberOperand(left);
    const b = numberOperand(right);
    return areIntegers(mode, a, b)
      ? checkInteger(finite(onIntegers(a, b)))
      : finite(onReals(a, b));
  };
}

/** Unary minus. */
export function negate(value: Value): Value {
  if (value === null) return null;
  const number = numberOperand(value);
  return number === 0 ? 0 : -number;
}

/** `left || right`: text of both sides, NULL if either is NULL. */
export function concatenate(left: Value, right: Value): Value {
  return left === null || right === null
    ? null
    : textOperand(left) + textOperand(right);
}

/**
 * The number an operand stands for: a number, or text that is exactly a
 * number (`'42'`, `'1.5'`). Anything else is a DATATYPE_MISMATCH.
 */
export function numberOperand(value: Exclude<Value, null>): number {
  if (typeof value === "number") return value;
  const number = typeof value === "string" ? numberFromText(value) : null;
  if (number === null) {
    throw new DatatypeMismatchError(`${describeValue(value)} is not a number`);
  }
  return number;
}

/**
 * The text an operand stands for: text itself, numbers as JavaScript prints
 * them, booleans as `true` / `false`, JSON as JSON text. BLOBs have no text.
 */
export function textOperand(value: Exclude<Value, null>): string {
  if (typeof value === "string") return value;
  if (value instanceof Uint8Array) {
    throw new DatatypeMismatchError(
      `${describeValue(value)} cannot be used as text`,
    );
  }
  return castValue(value, "TEXT") as string;
}

/** Fails with INTEGER_OVERFLOW unless `value` is a safe integer. */
export function checkInteger(value: number | null): number | null {
  if (value === null || Number.isSafeInteger(value)) return value;
  throw new IntegerOverflowError();
}

function areIntegers(mode: NumericMode, a: number, b: number): boolean {
  switch (mode) {
    case "integer": {
      return true;
    }
    case "real": {
      return false;
    }
    case "dynamic": {
      return Number.isSafeInteger(a) && Number.isSafeInteger(b);
    }
  }
}

function integerOperation(operator: ArithmeticOperator): Operation {
  switch (operator) {
    case "add": {
      return (a, b) => a + b;
    }
    case "subtract": {
      return (a, b) => a - b;
    }
    case "multiply": {
      return (a, b) => a * b;
    }
    case "divide": {
      return (a, b) => (b === 0 ? null : truncate(a / b));
    }
    case "remainder": {
      return remainder;
    }
  }
}

function realOperation(operator: ArithmeticOperator): Operation {
  switch (operator) {
    case "divide": {
      return (a, b) => (b === 0 ? null : a / b);
    }
    case "remainder": {
      // As in SQLite, both operands are truncated to integers first.
      return (a, b) => remainder(truncate(a), truncate(b));
    }
    case "add":
    case "subtract":
    case "multiply": {
      return integerOperation(operator);
    }
  }
}

/** `a % b` with the sign of the dividend; NULL for a zero divisor. */
function remainder(a: number, b: number): number | null {
  return b === 0 ? null : a % b;
}

/** Rounds toward zero, never to -0. */
function truncate(number: number): number {
  const truncated = Math.trunc(number);
  return truncated === 0 ? 0 : truncated;
}

/** NaN (Infinity - Infinity) is NULL, as in SQLite; -0 is 0. */
function finite(value: number | null): number | null {
  if (value === null || Number.isNaN(value)) return null;
  return value === 0 ? 0 : value;
}
