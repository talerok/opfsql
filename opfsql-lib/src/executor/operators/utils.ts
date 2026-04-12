import type {
  TableFilter,
  TableSchema,
  BoundConstantExpression,
  BoundParameterExpression,
} from '../../binder/types.js';
import { BoundExpressionClass } from '../../binder/types.js';
import type { PhysicalOperator, Tuple, Value } from '../types.js';
import { applyComparison, serializeValue } from '../evaluate/helpers.js';

/** Resolve a filter value expression (literal or $N parameter) to its runtime scalar. */
export function resolveFilterValue(
  constant: BoundConstantExpression | BoundParameterExpression,
  params?: readonly Value[],
): Value {
  if (constant.expressionClass === BoundExpressionClass.BOUND_CONSTANT) {
    return constant.value;
  }
  return params?.[constant.index] ?? null;
}

/** Batch sizes used across operators. */
export const SCAN_BATCH = 500;
export const JOIN_BATCH = 2000;

/** Drain all tuples from an operator into a flat array. */
export async function drainOperator(op: PhysicalOperator): Promise<Tuple[]> {
  const result: Tuple[] = [];
  while (true) {
    const batch = await op.next();
    if (!batch) break;
    for (const tuple of batch) result.push(tuple);
  }
  return result;
}

/** Serialize an array of values into a single dedup/grouping key. */
export function serializeKey(values: Value[]): string {
  return values.map(serializeValue).join('\x00');
}

/** Convert a storage row to a positional tuple using column IDs and schema. */
export function rowToTuple(
  row: Record<string, Value>,
  columnIds: number[],
  schema: TableSchema,
): Tuple {
  return columnIds.map((colId) => row[schema.columns[colId].name] ?? null);
}

/** Apply pushed-down table filters to a tuple. Returns true if tuple passes all filters. */
export function passesFilters(
  tuple: Tuple,
  filters: TableFilter[],
  columnIds: number[],
  params?: readonly Value[],
): boolean {
  for (const filter of filters) {
    const pos = columnIds.indexOf(filter.columnIndex);
    if (pos === -1) continue;
    const val = resolveFilterValue(filter.constant, params);
    if (applyComparison(tuple[pos], val, filter.comparisonType) !== true) {
      return false;
    }
  }
  return true;
}
