import type { ColumnBinding } from '../binder/types.js';

export type Resolver = (binding: ColumnBinding) => number | undefined;

export function buildResolver(layout: ColumnBinding[]): Resolver {
  const map = new Map<string, number>();
  for (let i = 0; i < layout.length; i++) {
    map.set(`${layout[i].tableIndex}:${layout[i].columnIndex}`, i);
  }
  return (binding) => map.get(`${binding.tableIndex}:${binding.columnIndex}`);
}
