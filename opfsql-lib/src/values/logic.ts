import { areEqual, compareValues } from "./order.js";
import type { Value } from "./value.js";

// Three-valued logic: a condition is true, false or unknown (NULL).

export type Truth = boolean | null;

export function and(a: Truth, b: Truth): Truth {
  if (a === false || b === false) return false;
  if (a === null || b === null) return null;
  return true;
}

export function or(a: Truth, b: Truth): Truth {
  if (a === true || b === true) return true;
  if (a === null || b === null) return null;
  return false;
}

export function not(a: Truth): Truth {
  return a === null ? null : !a;
}

/** `a <op> b` as SQL compares: NULL when either side is NULL. */
export function compareSql(a: Value, b: Value): number | null {
  return a === null || b === null ? null : compareValues(a, b);
}

/**
 * `a IS b`: like `=`, but two NULLs are equal and the result is never NULL —
 * which is exactly equality under the total order.
 */
export const isSame = areEqual;

/**
 * `needle IN (items)`: true if an item equals the needle; otherwise NULL if
 * the needle or any item is NULL; otherwise false. Empty items: false.
 */
export function isIn(needle: Value, items: readonly Value[]): Truth {
  if (items.length === 0) return false;
  if (needle === null) return null;
  const isFound = items.some(
    (item) => item !== null && compareValues(needle, item) === 0,
  );
  if (isFound) return true;
  return items.includes(null) ? null : false;
}
