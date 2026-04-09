import type { ColumnBinding } from '../binder/types.js';
import { ExecutorError } from './errors.js';

export type Resolver = (binding: ColumnBinding) => number;

/**
 * Builds a fast lookup function: ColumnBinding → position in tuple.
 * Constructed once per operator, used for every tuple.
 */
export function buildResolver(layout: ColumnBinding[]): Resolver {
  const map = new Map<string, number>();
  for (let i = 0; i < layout.length; i++) {
    map.set(`${layout[i].tableIndex}:${layout[i].columnIndex}`, i);
  }

  return (binding: ColumnBinding): number => {
    const pos = map.get(`${binding.tableIndex}:${binding.columnIndex}`);
    if (pos === undefined) {
      throw new ExecutorError(
        `Column binding {tableIndex:${binding.tableIndex}, columnIndex:${binding.columnIndex}} not found in layout`,
      );
    }
    return pos;
  };
}