import type { BoundExpression } from '../../binder/types.js';
import { collectColumnRefs } from './collect_column_refs.js';

export function expressionReferencesTable(
  expr: BoundExpression,
  tableIndex: number,
): boolean {
  return collectColumnRefs(expr).some((b) => b.tableIndex === tableIndex);
}
