import type { LogicalOperator } from '../../binder/types.js';

export function getOperatorTables(op: LogicalOperator): Set<number> {
  const tables = new Set<number>();
  collectOperatorTables(op, tables);
  return tables;
}

function collectOperatorTables(
  op: LogicalOperator,
  tables: Set<number>,
): void {
  if ('tableIndex' in op && typeof op.tableIndex === 'number') {
    tables.add(op.tableIndex);
  }
  for (const child of op.children) {
    collectOperatorTables(child, tables);
  }
}
