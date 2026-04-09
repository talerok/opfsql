import type {
  BoundExpression,
  BoundColumnRefExpression,
} from '../../binder/types.js';
import type { Value, Tuple } from '../types.js';
import type { Resolver } from '../resolve.js';

export function evalColumnRef(
  expr: BoundExpression,
  tuple: Tuple,
  resolver: Resolver,
): Value {
  const ref = expr as BoundColumnRefExpression;
  const pos = resolver(ref.binding);
  return tuple[pos] ?? null;
}
