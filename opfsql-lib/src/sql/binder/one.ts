import { InternalError } from "../../errors.js";

// ---------------------------------------------------------------------------
// Lookups that must find one thing: a column of a name, a subquery's
// column, a stored statement. None and more than one are told apart, so
// each can be reported as what it is.
// ---------------------------------------------------------------------------

/**
 * Item `index` of `items`, which the caller knows is there — lists checked
 * to be as long, a position read from the list's own layout. One missing is
 * a broken contract, never a value to make up.
 */
export function itemAt<T>(items: readonly T[], index: number, what: string): T {
  const item = items[index];
  if (item === undefined) {
    throw new InternalError(
      `${what} has no item ${String(index)} of ${String(items.length)}`,
    );
  }
  return item;
}

/** The one item of `items`, or null for none; for more, `many()` is thrown. */
export function oneOrNone<T>(items: readonly T[], many: () => Error): T | null {
  const [item = null, other] = items;
  if (other !== undefined) throw many();
  return item;
}

/** The one item of `items`; for none or more, `wrong()` is thrown. */
export function exactlyOne<T>(items: readonly T[], wrong: () => Error): T {
  const item = oneOrNone(items, wrong);
  if (item === null) throw wrong();
  return item;
}
