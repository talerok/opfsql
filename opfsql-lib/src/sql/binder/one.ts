// ---------------------------------------------------------------------------
// Lookups that must find one thing: a column of a name, a subquery's
// column, a stored statement. None and more than one are told apart, so
// each can be reported as what it is.
// ---------------------------------------------------------------------------

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
