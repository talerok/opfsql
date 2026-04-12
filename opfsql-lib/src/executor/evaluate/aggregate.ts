import type { BoundAggregateExpression } from '../../binder/types.js';
import type { Resolver } from '../resolve.js';
import type { Tuple, Value } from '../types.js';
import { ExecutorError } from '../errors.js';

export function evalAggregate(
  expr: BoundAggregateExpression,
  tuple: Tuple,
  resolver: Resolver,
): Value {
  const pos = resolver(expr.binding!);
  if (pos === undefined) {
    const { tableIndex, columnIndex } = expr.binding!;
    throw new ExecutorError(
      `Aggregate binding {tableIndex:${tableIndex}, columnIndex:${columnIndex}} not found in layout`,
    );
  }
  return tuple[pos] ?? null;
}
