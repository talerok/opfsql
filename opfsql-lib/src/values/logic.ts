import { DatatypeMismatchError } from "../errors.js";
import { describeValue } from "./convert.js";
import { compareValues } from "./order.js";
import type { Value } from "./value.js";

// Three-valued logic: a condition is true, false or unknown (NULL).

export type Truth = boolean | null;

/**
 * A value used as a condition: a BOOLEAN or NULL. Anything else is a
 * DATATYPE_MISMATCH — conditions are never judged by truthiness.
 */
export function truthOf(value: Value): Truth {
  if (value === null || typeof value === "boolean") return value;
  throw new DatatypeMismatchError(
    `${describeValue(value)} is not a BOOLEAN condition`,
  );
}

export function and(a: Truth, b: Truth): Truth {
  if (a === false || b === false) return false;
  return a === null || b === null ? null : true;
}

export function or(a: Truth, b: Truth): Truth {
  if (a === true || b === true) return true;
  return a === null || b === null ? null : false;
}

export function not(a: Truth): Truth {
  return a === null ? null : !a;
}

/** `a <op> b` as SQL compares: NULL when either side is NULL. */
export function compareSql(a: Value, b: Value): number | null {
  return a === null || b === null ? null : compareValues(a, b);
}

/** `a IS b`: like `=`, but two NULLs are equal and the result is never NULL. */
export function isSame(a: Value, b: Value): boolean {
  return compareValues(a, b) === 0;
}

/**
 * `needle IN (items)`: true if an item equals the needle; otherwise NULL if
 * the needle or any item is NULL; otherwise false. Empty items: false.
 */
export function isIn(needle: Value, items: readonly Value[]): Truth {
  if (items.length === 0) return false;
  if (needle === null) return null;
  let hasNull = false;
  for (const item of items) {
    if (item === null) hasNull = true;
    else if (compareValues(needle, item) === 0) return true;
  }
  return hasNull ? null : false;
}
