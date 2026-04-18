import type {
  TableSchema,
  BoundConstantExpression,
  BoundParameterExpression,
} from '../../binder/types.js';
import { BoundExpressionClass } from '../../binder/types.js';
import type { CompiledFilter } from '../evaluate/compile.js';
import type { SyncPhysicalOperator, Tuple, Value } from '../types.js';
import { applyComparison } from '../evaluate/utils/compare.js';
import { serializeValue } from '../evaluate/utils/serialize.js';

export const SCAN_BATCH = 500;
export const JOIN_BATCH = 2000;

export function resolveFilterValue(
  constant: BoundConstantExpression | BoundParameterExpression,
  params?: readonly Value[],
): Value {
  if (constant.expressionClass === BoundExpressionClass.BOUND_CONSTANT) return constant.value;
  return params?.[constant.index] ?? null;
}

export function serializeKey(values: Value[]): string {
  return values.map(serializeValue).join('\x00');
}

export function rowToTuple(
  row: Record<string, Value>,
  columnIds: number[],
  schema: TableSchema,
): Tuple {
  return columnIds.map((colId) => {
    const col = schema.columns[colId];
    const val = row[col.name];
    return val !== undefined ? val : (col.defaultValue ?? null);
  });
}

export function passesCompiledFilters(
  tuple: Tuple,
  filters: CompiledFilter[],
  params?: readonly Value[],
): boolean {
  for (const filter of filters) {
    const val = filter.getValue(tuple);
    const cmp = filter.getConstant(params);
    if (applyComparison(val, cmp, filter.comparisonType) !== true) return false;
  }
  return true;
}

export function drainOperator(op: SyncPhysicalOperator): Tuple[] {
  const result: Tuple[] = [];
  while (true) {
    const batch = op.next();
    if (!batch) break;
    for (const tuple of batch) result.push(tuple);
  }
  return result;
}
