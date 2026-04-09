import type { BoundExpression } from '../../binder/types.js';
import { collectColumnRefs } from './collect_column_refs.js';

export function getExpressionTables(expr: BoundExpression): Set<number> {
  const tables = new Set<number>();
  for (const ref of collectColumnRefs(expr)) {
    tables.add(ref.tableIndex);
  }
  return tables;
}
