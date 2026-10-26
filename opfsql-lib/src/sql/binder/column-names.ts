import { nameKey } from "../../schema/index.js";

// ---------------------------------------------------------------------------
// Column names of a table or of a statement's column list: found by
// nameKey, and each named once.
// ---------------------------------------------------------------------------

/** The position of the column called `name` among `columns`; -1 if none. */
export function findColumn(
  columns: readonly { readonly name: string }[],
  name: string,
): number {
  const key = nameKey(name);
  return columns.findIndex((column) => nameKey(column.name) === key);
}

/** The position of the first value met before; -1 if each is met once. */
export function firstRepeated(values: readonly unknown[]): number {
  return values.findIndex(
    (value, position) => values.indexOf(value) !== position,
  );
}
