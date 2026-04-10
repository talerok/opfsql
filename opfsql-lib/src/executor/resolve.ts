import type { ColumnBinding } from '../binder/types.js';

/**
 * Maps a ColumnBinding to its position in a tuple.
 * Returns undefined when the binding is not in this resolver's layout
 * (used by correlated subqueries to fall back to outer context).
 */
export type Resolver = (binding: ColumnBinding) => number | undefined;

/**
 * Builds a fast lookup function: ColumnBinding → position in tuple.
 * Constructed once per operator, used for every tuple.
 */
export function buildResolver(layout: ColumnBinding[]): Resolver {
  const map = new Map<string, number>();
  for (let i = 0; i < layout.length; i++) {
    map.set(`${layout[i].tableIndex}:${layout[i].columnIndex}`, i);
  }

  return (binding: ColumnBinding): number | undefined => {
    return map.get(`${binding.tableIndex}:${binding.columnIndex}`);
  };
}