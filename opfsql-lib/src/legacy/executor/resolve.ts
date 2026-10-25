import type { ColumnBinding } from "../binder/types.js";

/** Where a column is in a row: its position, or undefined if it is absent. */
export type Resolver = (binding: ColumnBinding) => number | undefined;

/** The resolver for rows whose columns are `layout`, in that order. */
export function buildResolver(layout: readonly ColumnBinding[]): Resolver {
  const positions = new Map<string, number>();
  for (const [position, binding] of layout.entries()) {
    positions.set(key(binding), position);
  }
  return (binding) => positions.get(key(binding));
}

function key(binding: ColumnBinding): string {
  return `${String(binding.tableIndex)}:${String(binding.columnIndex)}`;
}
