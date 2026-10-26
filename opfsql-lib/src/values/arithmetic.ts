import { DatatypeMismatchError, IntegerOverflowError } from "../errors.js";
import {
  castValue,
  describeValue,
  normalizeNumber,
  numberFromText,
  truncate,
  withoutNegativeZero,
} from "./convert.js";
import type { Value } from "./value.js";

// ---------------------------------------------------------------------------
// Arithmetic and concatenation (docs/semantics.md, Arithmetic). NULL in,
// NULL out; INTEGER results must stay safe integers; dividing by zero is NULL.
// ---------------------------------------------------------------------------

/** The binary arithmetic operators, `+ - * / %`, as plans name them. */
export type ArithmeticOperator =
  "ADD" | "SUBTRACT" | "MULTIPLY" | "DIVIDE" | "MOD";

/**
 * How operands are treated, by the static type of the result: `integer`
 * for INTEGER, `real` for REAL (an operand is REAL), `dynamic` when it has
 * none (ANY, NULL) and the values decide: two safe integers are integers.
 */
export type NumericMode = "integer" | "real" | "dynamic";

type Operation = (left: number, right: number) => number | null;

const add: Operation = (a, b) => a + b;
const subtract: Operation = (a, b) => a - b;
const multiply: Operation = (a, b) => a * b;

/** `a % b` with the sign of the dividend; NULL for a zero divisor. */
const remainder: Operation = (a, b) => (b === 0 ? null : a % b);

interface Operations {
  readonly integer: Operation;
  readonly real: Operation;
}

/** `operator` on two INTEGERs, and on numbers that are not. */
function operationsOf(operator: ArithmeticOperator): Operations {
  switch (operator) {
    case "ADD": {
      return { integer: add, real: add };
    }
    case "SUBTRACT": {
      return { integer: subtract, real: subtract };
    }
    case "MULTIPLY": {
      return { integer: multiply, real: multiply };
    }
    case "DIVIDE": {
      return {
        integer: (a, b) => (b === 0 ? null : truncate(a / b)),
        real: (a, b) => (b === 0 ? null : a / b),
      };
    }
    case "MOD": {
      return {
        integer: remainder,
        // As in SQLite, both operands are truncated to integers first.
        real: (a, b) => remainder(truncate(a), truncate(b)),
      };
    }
  }
}

/** The binary operator for `mode`, chosen once and applied per row. */
export function arithmetic(
  operator: ArithmeticOperator,
  mode: NumericMode,
): (left: Value, right: Value) => Value {
  const { integer, real } = operationsOf(operator);
  return (left, right) => {
    if (left === null || right === null) return null;
    const a = numberOperand(left);
    const b = numberOperand(right);
    const isInteger = areIntegers(mode, a, b);
    const result = isInteger ? integer(a, b) : real(a, b);
    if (result === null) return null;
    if (isInteger) return checkInteger(withoutNegativeZero(result));
    return normalizeNumber(result);
  };
}

/** Unary minus. */
export function negate(value: Value): Value {
  if (value === null) return null;
  return withoutNegativeZero(-numberOperand(value));
}

/** `left || right`: text of both sides, NULL if either is NULL. */
export function concatenate(left: Value, right: Value): Value {
  if (left === null || right === null) return null;
  return textOperand(left) + textOperand(right);
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
export function checkInteger(value: number): number {
  if (Number.isSafeInteger(value)) return value;
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
