import type { LogicalOperator } from '../../binder/types.js';
import { LogicalOperatorType } from '../../binder/types.js';

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
  // CTE_REF has no tableIndex property but its column bindings reference
  // the virtual table assigned during binding. Extract those so join order
  // and filter pushdown can correctly associate CTE refs with join edges.
  if (op.type === LogicalOperatorType.LOGICAL_CTE_REF) {
    for (const binding of op.getColumnBindings()) {
      tables.add(binding.tableIndex);
    }
  }
  for (const child of op.children) {
    collectOperatorTables(child, tables);
  }
}
